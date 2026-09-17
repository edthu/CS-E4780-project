import java.io.{BufferedReader, FileInputStream, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import scala.io.Source

/** Reads the ingestion NDJSON and publishes each line to a Kafka topic,
  * keyed by symbol so all events for a symbol land on one partition.
  *
  * The value is the raw NDJSON line forwarded verbatim; downstream stages
  * deserialise it. Input `-` reads stdin so ingestion can be piped directly.
  *
  * Modes:
  *   - batch (default): read to EOF, then exit. Good for a fixed sample.
  *   - follow (`PRODUCER_FOLLOW=true` or arg `--follow`): after draining the
  *     current file, stay alive and keep publishing lines that get appended
  *     later (like `tail -f`). Use this when ingestion keeps writing / more
  *     data can arrive at any time, so the pipeline never goes idle.
  * `PRODUCER_FOLLOW_INTERVAL_MS` (default 1000) sets the poll cadence in follow
  * mode. Follow is ignored for stdin, which already streams until closed.
  */
object ProducerApp:
  private val defaultInput = "/output/events.ndjson"

  def main(args: Array[String]): Unit =
    val input = args.find(a => !a.startsWith("--")).getOrElse(defaultInput)
    val follow =
      args.contains("--follow") ||
        sys.env.get("PRODUCER_FOLLOW").exists(v => v.equalsIgnoreCase("true") || v == "1")
    val followIntervalMs = sys.env
      .get("PRODUCER_FOLLOW_INTERVAL_MS")
      .flatMap(v => scala.util.Try(v.toLong).toOption)
      .filter(_ > 0)
      .getOrElse(1000L)
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
      s"[producer] bootstrap=$bootstrap topic=$topic input=$input follow=$follow"
    )
    var sent = 0L
    var skipped = 0L

    // Skip and count unparseable lines (e.g. a truncated final line while
    // ingestion is still writing) rather than aborting the whole run.
    def sendLine(line: String): Unit =
      try
        val key = EventJson.symbolOf(line)
        producer.send(new ProducerRecord[String, String](topic, key, line))
        sent += 1
        if sent % 100000 == 0 then println(s"[producer] sent=$sent")
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

    try
      if input == "-" then
        // stdin already streams until the writer closes it; follow is implicit.
        val source = Source.stdin
        try
          for line <- source.getLines() if line.trim.nonEmpty do sendLine(line)
        finally source.close()
      else
        // A BufferedReader keeps returning newly-appended lines after it first
        // hits EOF (as long as we don't close it), which is how we tail -f.
        val reader = new BufferedReader(
          new InputStreamReader(new FileInputStream(input), StandardCharsets.UTF_8)
        )
        try
          pump(reader)
          if follow then
            println(
              s"[producer] reached end of input; following for new data (sent=$sent, interval=${followIntervalMs}ms, Ctrl-C to stop)"
            )
            while true do
              Thread.sleep(followIntervalMs)
              pump(reader)
        finally reader.close()
    finally
      producer.flush()
      producer.close()
    println(s"[producer] completed sent=$sent skipped=$skipped")
