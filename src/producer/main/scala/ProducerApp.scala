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
  */
object ProducerApp:
  private val defaultInput = "/output/events.ndjson"

  def main(args: Array[String]): Unit =
    val input = args.headOption.getOrElse(defaultInput)
    val bootstrap = sys.env.getOrElse("KAFKA_BOOTSTRAP", "localhost:9092")
    val topic = sys.env.getOrElse("KAFKA_TOPIC", "trading-events")

    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.LINGER_MS_CONFIG, "20")

    val producer = new KafkaProducer[String, String](props)
    println(s"[producer] bootstrap=$bootstrap topic=$topic input=$input")
    var sent = 0L
    var skipped = 0L
    try
      val source =
        if input == "-" then Source.stdin
        else Source.fromFile(input, StandardCharsets.UTF_8.name)
      try
        for line <- source.getLines() if line.trim.nonEmpty do
          // Skip and count unparseable lines (e.g. a truncated final line while
          // ingestion is still writing) rather than aborting the whole run.
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
      finally source.close()
    finally
      producer.flush()
      producer.close()
    println(s"[producer] completed sent=$sent skipped=$skipped")
