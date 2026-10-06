import java.io.{BufferedReader, FileInputStream, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.time.{LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
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
  *
  * Throughput is logged every `PRODUCER_SUMMARY_INTERVAL_MS` (default 2000) as
  * records/second over the last interval and since start, mirroring
  * `ConsumerApp`'s summary line.
  */
object ProducerApp:
  private val defaultInput = "data"

  private final case class Options(inputs: Vector[String], speed: Double, follow: Boolean)

  def main(args: Array[String]): Unit =
    val options = parseOptions(args)
    val inputs = if options.inputs.isEmpty then Vector(defaultInput) else options.inputs
    val stdin = inputs.contains("-")
    require(!stdin || inputs.size == 1, "stdin cannot be combined with file inputs")
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

    def sendEvent(event: Event, raw: String): Unit =
      pacer.await(event.timestamp)
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
        val csvFiles = orderCsvFiles(paths.filter(isCsv))
        val otherFiles = paths.filterNot(isCsv)
        val orderedInputs = csvFiles ++ otherFiles
        require(orderedInputs.nonEmpty, s"no supported input files found in ${inputs.mkString(",")}")
        require(!options.follow || (orderedInputs.size == 1 && !isCsv(orderedInputs.head)),
          "--follow is supported only for a single NDJSON file")
        orderedInputs.foreach { path =>
          if isCsv(path) then
            val parser = CSVParser.parse(path, StandardCharsets.UTF_8, CsvEventParser.csvFormat)
            try
              parser.iterator().asScala.foreach { record =>
                CsvEventParser.readRecord(record) match
                  case ParseResult.Accepted(event) => sendEvent(event, EventJson.render(event))
                  case ParseResult.Skipped(_) => skippedNonPrice += 1
                  case ParseResult.Rejected(row) =>
                    rejected += 1
                    if rejected <= 10 then
                      System.err.println(s"[producer] rejected ${path}:${row.rowNumber}: ${row.reason}")
              }
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
    val avgRps = rate(sent, System.nanoTime() - startedAt)
    println(
      f"[producer] completed sent=$sent skipped=$skipped skippedNonPrice=$skippedNonPrice " +
        f"rejected=$rejected avgRecordsPerSecond=$avgRps%.1f"
    )

  // Files without any price events (e.g. weekend days) contribute nothing, so they are skipped.
  def orderCsvFiles(paths: Seq[Path]): Vector[Path] =
    paths.toVector
      .flatMap { path =>
        val first = firstValidEventTimestamp(path)
        if first.isEmpty then System.err.println(s"[producer] skipping CSV with no valid price events: $path")
        first.map(path -> _)
      }
      .sortBy(_._2)
      .map(_._1)

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
    var index = 0
    while index < args.length do
      args(index) match
        case "--speed" =>
          require(index + 1 < args.length, "--speed requires a multiplier")
          speed = args(index + 1).toDouble
          index += 1
        case "--follow" => follow = true
        case input => inputs += input
      index += 1
    require(speed.isFinite && speed >= 0, s"speed must be a finite non-negative multiplier: $speed")
    Options(inputs.toVector, speed, follow)

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
