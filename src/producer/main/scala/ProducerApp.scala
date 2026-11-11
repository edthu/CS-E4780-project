import java.io.{BufferedReader, BufferedWriter, FileInputStream, FileWriter, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.time.{LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.common.utils.Utils
import org.apache.commons.csv.CSVParser
import scala.io.Source
import scala.jdk.CollectionConverters.*

/** Streams CSV or NDJSON events to Kafka, keyed by symbol.
  *
  * CSV directories are ordered by their first valid event timestamp. Events
  * are paced against source timestamps; --speed 0 disables pacing.
  *
  * Modes:
  *   - CSV inputs: one or more files, or a directory containing daily CSVs.
  *   - NDJSON input: read line by line; `-` reads stdin.
  *   - `--follow` retains the previous append-follow mode for NDJSON files.
  *   - `--from` / `--to` (or `PRODUCER_FROM` / `PRODUCER_TO`) replay only the CSV
  *     events in [from, to), in Europe/Amsterdam local time, e.g.
  *     `2021-11-11T11:30:00`. Pacing starts at the first event kept.
  *
  * Benchmarks can set `PRODUCER_PROBES` (probe symbols per partition) and
  * `PRODUCER_PROBE_LOG`; see [[ProbeInjector]].
  *
  * Throughput is logged every `PRODUCER_SUMMARY_INTERVAL_MS` (default 2000) as
  * records/second over the last interval and since start.
  */
object ProducerApp:
  private val defaultInput = "data"

  private final case class Options(inputs: Vector[String], speed: Double, follow: Boolean, range: ReplayRange)

  def main(args: Array[String]): Unit =
    val options = parseOptions(args)
    val inputs = if options.inputs.isEmpty then Vector(defaultInput) else options.inputs
    val stdin = inputs.contains("-")
    require(!stdin || inputs.size == 1, "stdin cannot be combined with file inputs")
    require(!stdin || options.range.isUnbounded, "--from/--to are supported only for CSV inputs")
    val followIntervalMs = sys.env
      .get("PRODUCER_FOLLOW_INTERVAL_MS")
      .flatMap(v => scala.util.Try(v.toLong).toOption)
      .filter(_ > 0)
      .getOrElse(1000L)
    val summaryIntervalMs = sys.env
      .get("PRODUCER_SUMMARY_INTERVAL_MS")
      .flatMap(v => scala.util.Try(v.toLong).toOption)
      .filter(_ > 0)
      .getOrElse(2000L)
    val bootstrap = sys.env.getOrElse("KAFKA_BOOTSTRAP", "localhost:9092")
    val topic = sys.env.getOrElse("KAFKA_TOPIC", "trading-events")

    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.LINGER_MS_CONFIG, "20")

    val producer = new KafkaProducer[String, String](props)
    println(
      s"[producer] bootstrap=$bootstrap topic=$topic inputs=${inputs.mkString(",")} " +
        s"speed=${options.speed}x follow=${options.follow}"
    )
    var sent = 0L
    var skipped = 0L
    var skippedNonPrice = 0L
    var rejected = 0L
    val pacer = new ReplayPacer(options.speed)
    val probesPerPartition = sys.env.get("PRODUCER_PROBES").flatMap(_.toIntOption).filter(_ > 0).getOrElse(0)
    val probeLog = sys.env.get("PRODUCER_PROBE_LOG").map(_.trim).filter(_.nonEmpty)
    require(probesPerPartition == 0 || probeLog.nonEmpty, "PRODUCER_PROBES requires PRODUCER_PROBE_LOG")
    val probes =
      if probesPerPartition == 0 then None
      else
        val symbols = ProbeInjector.symbolsFor(producer.partitionsFor(topic).size, probesPerPartition)
        println(s"[producer] probes=${symbols.size} log=${probeLog.get}")
        Some(new ProbeInjector(symbols))
    val probeWriter = probeLog.filter(_ => probes.nonEmpty).map(path => new BufferedWriter(new FileWriter(path, true)))
    val startedAt = System.nanoTime()
    var lastSummaryAt = startedAt
    var lastSummaryCount = 0L

    def rate(delta: Long, nanos: Long): Double =
      if nanos <= 0 then 0.0 else delta * 1e9 / nanos

    // Called per record and after every drain, so it also fires while follow
    // mode is idle; silent when nothing new was sent.
    def maybeSummary(): Unit =
      val now = System.nanoTime()
      if now - lastSummaryAt >= summaryIntervalMs * 1000000L && sent > lastSummaryCount then
        val intervalRps = rate(sent - lastSummaryCount, now - lastSummaryAt)
        val avgRps = rate(sent, now - startedAt)
        println(
          f"[producer] sent=$sent recordsPerSecond=$intervalRps%.1f avgRecordsPerSecond=$avgRps%.1f"
        )
        lastSummaryAt = now
        lastSummaryCount = sent

    // Runs before the event that crosses a window boundary is sent, so the
    // recorded time precedes whatever closes the window on each partition.
    def sendProbes(event: Event): Unit =
      for injector <- probes; round <- injector.observe(ReplayPacer.epochMillis(event.timestamp)) do
        injector.symbols.foreach { symbol =>
          val t0 = System.currentTimeMillis()
          val probe = Event(symbol, "E", ProbeInjector.Price, event.timestamp)
          producer.send(new ProducerRecord[String, String](topic, symbol, EventJson.render(probe)))
          round.foreach(window => probeWriter.foreach(_.write(s"$window,$symbol,$t0\n")))
        }
        probeWriter.foreach(_.flush())

    def sendEvent(event: Event, raw: String): Unit =
      pacer.await(event.timestamp)
      sendProbes(event)
      producer.send(new ProducerRecord[String, String](topic, event.symbol, raw))
      sent += 1
      maybeSummary()

    def sendLine(line: String): Unit =
      try
        val event = EventJson.parse(line)
        sendEvent(event, line)
      catch
        case e: Exception =>
          skipped += 1
          if skipped <= 10 then
            System.err.println(s"[producer] skipping unparseable line: ${e.getMessage}")

    // Drain everything currently readable, flushing so records leave promptly.
    def pump(reader: BufferedReader): Unit =
      var line = reader.readLine()
      while line != null do
        if line.trim.nonEmpty then sendLine(line)
        line = reader.readLine()
      producer.flush()
      maybeSummary()

    try
      if stdin then
        val source = Source.stdin
        try
          for line <- source.getLines() if line.trim.nonEmpty do sendLine(line)
        finally source.close()
      else
        val paths = discoverInputs(inputs.map(Paths.get(_)))
        val csvFiles = ReplayRange.selectFiles(csvFilesWithStart(paths.filter(isCsv)), options.range)
        val otherFiles = paths.filterNot(isCsv)
        require(otherFiles.isEmpty || options.range.isUnbounded, "--from/--to are supported only for CSV inputs")
        println(s"[producer] selected CSV files: ${csvFiles.map(_.getFileName).mkString(",")}")
        val orderedInputs = csvFiles ++ otherFiles
        require(orderedInputs.nonEmpty, s"no supported input files found in ${inputs.mkString(",")}")
        require(!options.follow || (orderedInputs.size == 1 && !isCsv(orderedInputs.head)),
          "--follow is supported only for a single NDJSON file")
        orderedInputs.foreach { path =>
          if isCsv(path) then
            val parser = CsvSeek.open(path, options.range)
            try
              val lastColumn = parser.getHeaderNames.asScala.indexWhere(_.trim.equalsIgnoreCase("last"))
              val records = parser.iterator().asScala
              val endDetector = new ReplayEndDetector(options.range)
              var pastEnd = false
              while records.hasNext && !pastEnd do
                val record = records.next()
                // Most rows are not price events; this avoids building their field map.
                val nonPrice = lastColumn >= 0 && (record.size <= lastColumn || record.get(lastColumn).isBlank)
                if nonPrice then skippedNonPrice += 1
                else CsvEventParser.readRecord(record) match
                  case ParseResult.Accepted(event) =>
                    if options.range.isUnbounded then sendEvent(event, EventJson.render(event))
                    else
                      val eventMillis = ReplayPacer.epochMillis(event.timestamp)
                      if options.range.contains(eventMillis) then sendEvent(event, EventJson.render(event))
                      pastEnd = endDetector.observe(eventMillis)
                  case ParseResult.Skipped(_) => skippedNonPrice += 1
                  case ParseResult.Rejected(row) =>
                    rejected += 1
                    if rejected <= 10 then
                      System.err.println(s"[producer] rejected ${path}:${row.rowNumber}: ${row.reason}")
            finally parser.close()
          else
            val reader = new BufferedReader(
              new InputStreamReader(new FileInputStream(path.toFile), StandardCharsets.UTF_8)
            )
            try
              pump(reader)
              if options.follow then
                println(
                  s"[producer] reached end of input; following for new data (sent=$sent, interval=${followIntervalMs}ms, Ctrl-C to stop)"
                )
                while true do
                  Thread.sleep(followIntervalMs)
                  pump(reader)
            finally reader.close()
        }
    finally
      producer.flush()
      producer.close()
      probeWriter.foreach(_.close())
    val avgRps = rate(sent, System.nanoTime() - startedAt)
    println(
      f"[producer] completed sent=$sent skipped=$skipped skippedNonPrice=$skippedNonPrice " +
        f"rejected=$rejected avgRecordsPerSecond=$avgRps%.1f"
    )

  // Files without any price events (e.g. weekend days) contribute nothing, so they are skipped.
  def orderCsvFiles(paths: Seq[Path]): Vector[Path] = csvFilesWithStart(paths).map(_._1)

  /** CSV files with their first valid event time, oldest first. */
  def csvFilesWithStart(paths: Seq[Path]): Vector[(Path, Long)] =
    paths.toVector
      .flatMap { path =>
        val first = firstValidEventTimestamp(path)
        if first.isEmpty then System.err.println(s"[producer] skipping CSV with no valid price events: $path")
        first.map(path -> _)
      }
      .sortBy(_._2)

  private def discoverInputs(inputs: Seq[Path]): Vector[Path] =
    inputs.toVector.flatMap { path =>
      if Files.isDirectory(path) then
        val stream = Files.list(path)
        try stream.iterator().asScala.filter(p => Files.isRegularFile(p) && isCsv(p)).toVector
        finally stream.close()
      else if Files.isRegularFile(path) then Vector(path)
      else throw new IllegalArgumentException(s"input does not exist or is not a file/directory: $path")
    }

  private def isCsv(path: Path): Boolean =
    path.getFileName.toString.toLowerCase.endsWith(".csv")

  private def firstValidEventTimestamp(path: Path): Option[Long] =
    val parser = CSVParser.parse(path, StandardCharsets.UTF_8, CsvEventParser.csvFormat)
    try
      val records = parser.iterator().asScala
      var timestamp: Option[Long] = None
      while records.hasNext && timestamp.isEmpty do
        CsvEventParser.readRecord(records.next()) match
          case ParseResult.Accepted(event) => timestamp = Some(ReplayPacer.epochMillis(event.timestamp))
          case _ => ()
      timestamp
    finally parser.close()

  private def parseOptions(args: Array[String]): Options =
    val inputs = scala.collection.mutable.ArrayBuffer.empty[String]
    val configuredSpeed = sys.env.get("PRODUCER_SPEED").flatMap(_.toDoubleOption).getOrElse(0.0)
    var speed = configuredSpeed
    var follow = sys.env.get("PRODUCER_FOLLOW").exists(v => v.equalsIgnoreCase("true") || v == "1")
    var from = ReplayRange.parseBound(sys.env.getOrElse("PRODUCER_FROM", ""))
    var to = ReplayRange.parseBound(sys.env.getOrElse("PRODUCER_TO", ""))
    var index = 0
    while index < args.length do
      args(index) match
        case "--speed" =>
          require(index + 1 < args.length, "--speed requires a multiplier")
          speed = args(index + 1).toDouble
          index += 1
        case "--follow" => follow = true
        case "--from" =>
          require(index + 1 < args.length, "--from requires a timestamp")
          from = ReplayRange.parseBound(args(index + 1))
          index += 1
        case "--to" =>
          require(index + 1 < args.length, "--to requires a timestamp")
          to = ReplayRange.parseBound(args(index + 1))
          index += 1
        case input => inputs += input
      index += 1
    require(speed.isFinite && speed >= 0, s"speed must be a finite non-negative multiplier: $speed")
    val range = ReplayRange(from, to)
    require(!follow || range.isUnbounded, "--from/--to cannot be combined with --follow")
    Options(inputs.toVector, speed, follow, range)

/** Half-open replay interval [from, to) in epoch milliseconds. */
final case class ReplayRange(from: Option[Long], to: Option[Long]):
  from.zip(to).foreach { case (start, end) => require(start < end, "--from must be before --to") }

  def isUnbounded: Boolean = from.isEmpty && to.isEmpty
  def contains(millis: Long): Boolean = from.forall(millis >= _) && to.forall(millis < _)

object ReplayRange:
  /** How far past `to` an event must be before it counts towards stopping. */
  val StopMarginMs = 60000L

  /** Consecutive events past the margin needed to stop, so that one outlier
    * timestamp cannot end a replay early.
    */
  val StopAfterEvents = 1000

  def parseBound(value: String): Option[Long] =
    Option(value).map(_.trim).filter(_.nonEmpty).map(ReplayPacer.epochMillis)

  /** Drops files that cannot contain events in `range`. Input must be sorted by
    * first event time, and a file is assumed to end where the next one starts.
    */
  def selectFiles(files: Vector[(Path, Long)], range: ReplayRange): Vector[Path] =
    val nextStarts = files.drop(1).map(file => Option(file._2)) :+ None
    files.zip(nextStarts).collect {
      case ((path, start), nextStart)
          if range.to.forall(start < _) && !nextStart.exists(next => range.from.exists(next <= _)) =>
        path
    }

/** Opens a CSV positioned close to the start of a replay range.
  *
  * Parsing the hours before the range would dominate the run, so this scans raw
  * lines until the system date and time columns reach `from` minus a margin. The
  * margin absorbs rows whose event time is ahead of their system time. System
  * time is not strictly ordered and stray late rows occur early in a file, so
  * the seek stops only after `ConfirmRows` consecutive rows past that point, and
  * byte-offset bisection would not be safe.
  */
object CsvSeek:
  val MarginMs = 300000L
  val ConfirmRows = 1000
  private val BufferChars = 1 << 22
  private val keyFormat = DateTimeFormatter.ofPattern("yyyyMMddHH:mm:ss.SSS")

  def open(path: Path, range: ReplayRange, confirmRows: Int = ConfirmRows): CSVParser =
    range.from match
      case None => CSVParser.parse(path, StandardCharsets.UTF_8, CsvEventParser.csvFormat)
      case Some(from) =>
        val reader = new BufferedReader(
          new InputStreamReader(new FileInputStream(path.toFile), StandardCharsets.UTF_8),
          BufferChars
        )
        try
          var header = reader.readLine()
          while header != null && header.startsWith("#") do header = reader.readLine()
          require(header != null, s"CSV has no header: $path")
          val names = header.split(",", -1)
          skipTo(reader, names, startKey(from), confirmRows)
          val format = CsvEventParser.csvFormat.builder().setHeader(names*).setSkipHeaderRecord(false).build()
          new CSVParser(reader, format)
        catch
          case e: Exception =>
            reader.close()
            throw e

  private def startKey(from: Long): String =
    keyFormat.format(java.time.Instant.ofEpochMilli(from - MarginMs).atZone(ZoneId.of("Europe/Amsterdam")))

  /** Leaves `reader` at the first line of the first run of `confirmRows` rows
    * at or after `key`. Rows without a timestamp neither extend nor break a run.
    */
  private def skipTo(reader: BufferedReader, names: Array[String], key: String, confirmRows: Int): Unit =
    val dateColumn = names.indexWhere(_.trim.equalsIgnoreCase("date"))
    val timeColumn = names.indexWhere(_.trim.equalsIgnoreCase("time"))
    require(dateColumn >= 0 && timeColumn >= 0, "CSV needs Date and Time columns to seek")
    var run = 0
    var searching = true
    while searching do
      if run == 0 then reader.mark(BufferChars)
      val line = reader.readLine()
      if line == null then
        if run > 0 then reader.reset()
        searching = false
      else
        systemTimeKey(line, dateColumn, timeColumn) match
          case Some(rowKey) if rowKey >= key =>
            run += 1
            if run >= confirmRows then
              reader.reset()
              searching = false
          case Some(_) => run = 0
          case None => ()

  /** "yyyyMMddHH:mm:ss.ssss", or None for comments and rows with no timestamp. */
  def systemTimeKey(line: String, dateColumn: Int, timeColumn: Int): Option[String] =
    if line.startsWith("#") then None
    else
      val date = column(line, dateColumn)
      val time = column(line, timeColumn)
      if date.length == 10 && time.nonEmpty then Some(date.substring(6) + date.substring(3, 5) + date.substring(0, 2) + time)
      else None

  private def column(line: String, index: Int): String =
    var start = 0
    var remaining = index
    while remaining > 0 && start >= 0 do
      val comma = line.indexOf(',', start)
      start = if comma < 0 then -1 else comma + 1
      remaining -= 1
    if start < 0 then ""
    else
      val end = line.indexOf(',', start)
      (if end < 0 then line.substring(start) else line.substring(start, end)).trim

/** Decides when a CSV file can stop being read because `range` has ended. */
final class ReplayEndDetector(range: ReplayRange):
  private var pastEnd = 0

  def observe(eventMillis: Long): Boolean =
    if range.to.exists(eventMillis >= _ + ReplayRange.StopMarginMs) then pastEnd += 1
    else pastEnd = 0
    pastEnd >= ReplayRange.StopAfterEvents

/** Tells the producer when to emit latency probes.
  *
  * Each round puts one event per probe symbol in the window that is starting,
  * so the next round closes that window on every partition. A round triggered
  * by a boundary reports the window it closes; the first round closes nothing.
  */
final class ProbeInjector(val symbols: Vector[String]):
  private var current: Option[Long] = None

  def observe(eventMillis: Long): Option[Option[Long]] =
    val window = ProbeInjector.windowStart(eventMillis)
    current match
      case None =>
        current = Some(window)
        Some(None)
      case Some(open) if window > open =>
        current = Some(window)
        Some(Some(open))
      case _ => None

object ProbeInjector:
  val WindowMs = 300000L
  val Price = 100.0

  def windowStart(eventMillis: Long): Long = eventMillis - Math.floorMod(eventMillis, WindowMs)

  /** `perPartition` probe symbols for every partition, using the partition
    * choice of Kafka's default partitioner for keyed records.
    */
  def symbolsFor(partitions: Int, perPartition: Int): Vector[String] =
    require(partitions > 0 && perPartition > 0, "partitions and probes per partition must be positive")
    Iterator
      .from(0)
      .map(i => f"PROBE$i%04d.T")
      .scanLeft((Vector.empty[String], Map.empty[Int, Int])) { case ((picked, counts), symbol) =>
        val partition = partitionOf(symbol, partitions)
        val used = counts.getOrElse(partition, 0)
        if used < perPartition then (picked :+ symbol, counts.updated(partition, used + 1))
        else (picked, counts)
      }
      .collectFirst { case (picked, _) if picked.size == partitions * perPartition => picked }
      .get

  def partitionOf(symbol: String, partitions: Int): Int =
    Utils.toPositive(Utils.murmur2(symbol.getBytes(StandardCharsets.UTF_8))) % partitions

object ReplayPacer:
  def epochMillis(timestamp: String): Long =
    LocalDateTime.parse(timestamp, DateTimeFormatter.ISO_LOCAL_DATE_TIME)
      .atZone(ZoneId.of("Europe/Amsterdam"))
      .toInstant
      .toEpochMilli

  def delayNanos(origin: String, current: String, speed: Double): Long =
    delayNanos(epochMillis(origin), epochMillis(current), speed)

  def delayNanos(originMillis: Long, currentMillis: Long, speed: Double): Long =
    require(speed.isFinite && speed >= 0, s"speed must be a finite non-negative multiplier: $speed")
    if speed == 0 then 0L
    else
      val deltaMillis = currentMillis - originMillis
      if deltaMillis <= 0 then 0L
      else math.min(Long.MaxValue.toDouble, deltaMillis.toDouble * 1000000.0 / speed).toLong

final class ReplayPacer(speed: Double):
  require(speed.isFinite && speed >= 0, s"speed must be a finite non-negative multiplier: $speed")
  private var origin: Option[(Long, Long)] = None

  def await(timestamp: String): Unit =
    val eventTime = ReplayPacer.epochMillis(timestamp)
    if origin.isEmpty then origin = Some((eventTime, System.nanoTime()))
    origin.foreach { case (originMillis, wallStartNanos) =>
      val targetElapsedNanos = ReplayPacer.delayNanos(originMillis, eventTime, speed)
      val elapsedNanos = System.nanoTime() - wallStartNanos
      val delay = targetElapsedNanos - elapsedNanos
      if delay > 0 then
        val millis = delay / 1000000L
        val nanos = (delay % 1000000L).toInt
        Thread.sleep(millis, nanos)
    }
