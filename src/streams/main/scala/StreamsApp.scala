import java.util.Properties
import java.util.concurrent.CountDownLatch
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig, Topology}
import org.apache.kafka.streams.kstream.{Consumed, KStream, Produced, ValueMapper}

/** Kafka Streams application.
  *
  * Today it only stamps a marker field onto every event. This stateless
  * `mapValues` is the deliberate seam for the final project: Query 1 (per-symbol
  * EMA over 5-minute tumbling windows) and Query 2 (crossover buy/sell
  * advisories) will replace it.
  */
object StreamsApp:
  val Marker = "processed by kafka streams"

  /** Pure transform: add `processedBy` while preserving the original fields. */
  def addMarker(value: String): String =
    val obj = ujson.read(value)
    obj("processedBy") = Marker
    obj.render()

  def buildTopology(inputTopic: String, outputTopic: String): Topology =
    val builder = new StreamsBuilder()
    val stringSerde = Serdes.String()
    val source: KStream[String, String] =
      builder.stream(inputTopic, Consumed.`with`(stringSerde, stringSerde))
    source
      // NOTE: future home of EMA windowing (Query 1) and crossover
      // advisory detection (Query 2).
      .mapValues(((v: String) => addMarker(v)): ValueMapper[String, String])
      .to(outputTopic, Produced.`with`(stringSerde, stringSerde))
    builder.build()

  def main(args: Array[String]): Unit =
    val bootstrap = sys.env.getOrElse("KAFKA_BOOTSTRAP", "localhost:9092")
    val appId = sys.env.getOrElse("KAFKA_APP_ID", "trading-streams")
    val inputTopic = sys.env.getOrElse("KAFKA_INPUT_TOPIC", "trading-events")
    val outputTopic = sys.env.getOrElse("KAFKA_OUTPUT_TOPIC", "trading-events-processed")

    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId)
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass.getName)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass.getName)

    val streams = new KafkaStreams(buildTopology(inputTopic, outputTopic), props)
    println(s"[streams] appId=$appId bootstrap=$bootstrap in=$inputTopic out=$outputTopic")

    // Block the main thread until shutdown; otherwise main returns and the JVM
    // exits before the stream threads process anything.
    val latch = new CountDownLatch(1)
    Runtime.getRuntime.addShutdownHook(new Thread("streams-shutdown") {
      override def run(): Unit =
        streams.close()
        latch.countDown()
    })
    streams.start()
    println("[streams] running (Ctrl-C to stop)")
    latch.await()
