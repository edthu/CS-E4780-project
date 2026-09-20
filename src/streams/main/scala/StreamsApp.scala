import java.nio.charset.StandardCharsets
import java.time.{Duration, Instant, LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.Properties
import java.util.concurrent.CountDownLatch
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsBuilder, StreamsConfig, Topology}
import org.apache.kafka.streams.kstream.{Consumed, KStream, Materialized, Produced, TimeWindows, Transformer, TransformerSupplier, ValueMapper, Windowed}
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore

/** Kafka Streams application.
  *
  * The stream enriches each input event with EMA state and emits advisories for
  * BUY/SELL crossovers. This follows the project assignment pattern: 5-minute tumbling windows over the
  * raw trading events, using the last price in each window as the closing value,
  * and then evaluating the EMA signals on the resulting window close.
  */
object StreamsApp:
  val AdvisoryTopic = "advisories"
  val WindowSize = Duration.ofMinutes(5)
  val CestZone = ZoneId.of("Europe/Amsterdam")

  final case class EmaState(emaFast: Double = 0.0, emaSlow: Double = 0.0)

  final case class WindowAccumulator(lastClose: Double = 0.0, lastTimestampMs: Long = Long.MinValue):
    def update(raw: String): WindowAccumulator =
      val obj = ujson.read(raw)
      val timestamp = obj("timestamp").str
      val eventTimeMs = timestampToEpochMillis(timestamp)
      if eventTimeMs >= lastTimestampMs then
        WindowAccumulator(obj("price").num, eventTimeMs)
      else this

  def enrichProcessed(value: String, emaFast: Double, emaSlow: Double): String =
    val obj = ujson.read(value)
    obj("ema38") = emaFast
    obj("ema100") = emaSlow
    obj.render()

  def calculateEma(previous: Double, current: Double, period: Int): Double =
    if previous == 0.0 then current
    else
      val alpha = 2.0 / (period + 1.0)
      alpha * current + (1.0 - alpha) * previous

  def detectSignal(
      previousFast: Double,
      previousSlow: Double,
      currentFast: Double,
      currentSlow: Double
  ): Option[String] =
    if currentFast > currentSlow && previousFast <= previousSlow then Some("BUY")
    else if currentFast < currentSlow && previousFast >= previousSlow then Some("SELL")
    else None

  private def timestampToEpochMillis(timestamp: String): Long =
    try
      Instant.parse(timestamp).toEpochMilli()
    catch
      case _: Exception =>
        val local = LocalDateTime.parse(timestamp, DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS"))
        local.atZone(CestZone).toInstant.toEpochMilli()

  private def windowStartMs(timestamp: String): Long =
    val epochMs = timestampToEpochMillis(timestamp)
    epochMs - (epochMs % WindowSize.toMillis)

  private def parseState(raw: String): EmaState =
    if raw == null || raw.isBlank then EmaState()
    else
      val obj = ujson.read(raw)
      EmaState(
        emaFast = obj("ema38").num,
        emaSlow = obj("ema100").num
      )

  private def renderState(state: EmaState): String =
    ujson.Obj("ema38" -> state.emaFast, "ema100" -> state.emaSlow).render()

  private val windowAccumulatorSerde = Serdes.serdeFrom[WindowAccumulator](
    new org.apache.kafka.common.serialization.Serializer[WindowAccumulator] {
      override def serialize(topic: String, value: WindowAccumulator): Array[Byte] =
        if value == null then null
        else
          ujson.Obj(
            "lastClose" -> value.lastClose,
            "lastTimestampMs" -> value.lastTimestampMs.toDouble
          ).render().getBytes(StandardCharsets.UTF_8)
    },
    new org.apache.kafka.common.serialization.Deserializer[WindowAccumulator] {
      override def deserialize(topic: String, data: Array[Byte]): WindowAccumulator =
        if data == null then null
        else
          val obj = ujson.read(new String(data, StandardCharsets.UTF_8))
          WindowAccumulator(obj("lastClose").num, obj("lastTimestampMs").num.toLong)
    }
  )

  private final class WindowSignalTransformer extends Transformer[String, String, KeyValue[String, String]]:
    private var store: KeyValueStore[String, String] = _

    override def init(context: ProcessorContext): Unit =
      this.store = context.getStateStore("window-ema-state").asInstanceOf[KeyValueStore[String, String]]

    override def transform(symbol: String, value: String): KeyValue[String, String] =
      val obj = ujson.read(value)
      val close = obj("lastClose").num
      val previousState = parseState(store.get(symbol))
      val nextFast = calculateEma(previousState.emaFast, close, 38)
      val nextSlow = calculateEma(previousState.emaSlow, close, 100)
      val signal = detectSignal(previousState.emaFast, previousState.emaSlow, nextFast, nextSlow)
      store.put(symbol, renderState(EmaState(nextFast, nextSlow)))

      signal match
        case Some(label) =>
          val advisory = ujson.Obj(
            "symbol" -> symbol,
            "signal" -> label,
            "ema38" -> nextFast,
            "ema100" -> nextSlow,
            "close" -> close,
            "windowStart" -> obj("windowStart").num,
            "windowEnd" -> obj("windowEnd").num
          )
          KeyValue.pair(symbol, advisory.render())
        case None => null

    override def close(): Unit = ()

  def emittedProcessedRecord(value: String, emaFast: Double, emaSlow: Double): String =
    enrichProcessed(value, emaFast, emaSlow)

  def buildTopology(inputTopic: String, outputTopic: String, advisoryTopic: String = AdvisoryTopic): Topology =
    val builder = new StreamsBuilder()
    val stringSerde = Serdes.String()
    builder.addStateStore(
      org.apache.kafka.streams.state.Stores.keyValueStoreBuilder(
        org.apache.kafka.streams.state.Stores.persistentKeyValueStore("record-ema-state"),
        Serdes.String(),
        Serdes.String()
      )
    )
    builder.addStateStore(
      org.apache.kafka.streams.state.Stores.keyValueStoreBuilder(
        org.apache.kafka.streams.state.Stores.persistentKeyValueStore("window-ema-state"),
        Serdes.String(),
        Serdes.String()
      )
    )

    val source: KStream[String, String] =
      builder.stream(inputTopic, Consumed.`with`(stringSerde, stringSerde))

    val processedSupplier = new TransformerSupplier[String, String, KeyValue[String, String]] {
      override def get(): Transformer[String, String, KeyValue[String, String]] =
        new Transformer[String, String, KeyValue[String, String]] {
          private var store: KeyValueStore[String, String] = _

          override def init(context: ProcessorContext): Unit =
            this.store = context.getStateStore("record-ema-state").asInstanceOf[KeyValueStore[String, String]]

          override def transform(key: String, value: String): KeyValue[String, String] =
            val obj = ujson.read(value)
            val symbol = obj("symbol").str
            val close = obj("price").num
            val prev = parseState(store.get(symbol))
            val fast = calculateEma(prev.emaFast, close, 38)
            val slow = calculateEma(prev.emaSlow, close, 100)
            store.put(symbol, renderState(EmaState(fast, slow)))
            KeyValue.pair(symbol, enrichProcessed(value, fast, slow))

          override def close(): Unit = ()
        }
    }

    val enrichedProcessed = source.transform(processedSupplier, "record-ema-state")

    enrichedProcessed.to(outputTopic, Produced.`with`(stringSerde, stringSerde))

    val windows = source
      .groupByKey()
      .windowedBy(TimeWindows.ofSizeWithNoGrace(WindowSize))
      .aggregate(
        () => WindowAccumulator(),
        (_, raw: String, current: WindowAccumulator) => current.update(raw),
        Materialized.`with`(stringSerde, windowAccumulatorSerde)
      )
      .toStream()
      .map((windowedKey: Windowed[String], acc: WindowAccumulator) =>
        val symbol = windowedKey.key()
        val record = ujson.Obj(
          "symbol" -> symbol,
          "lastClose" -> acc.lastClose,
          "windowStart" -> windowedKey.window().start().toDouble,
          "windowEnd" -> windowedKey.window().end().toDouble
        )
        KeyValue.pair(symbol, record.render())
      )

    val advisoriesSupplier = new TransformerSupplier[String, String, KeyValue[String, String]] {
      override def get(): Transformer[String, String, KeyValue[String, String]] =
        new WindowSignalTransformer()
    }

    val advisories = windows.transform(advisoriesSupplier, "window-ema-state")
    advisories
      .filter((_: String, v: String) => v != null)
      .to(advisoryTopic, Produced.`with`(stringSerde, stringSerde))

    builder.build()

  def main(args: Array[String]): Unit =
    val bootstrap = sys.env.getOrElse("KAFKA_BOOTSTRAP", "localhost:9092")
    val appId = sys.env.getOrElse("KAFKA_APP_ID", "trading-streams")
    val inputTopic = sys.env.getOrElse("KAFKA_INPUT_TOPIC", "trading-events")
    val outputTopic = sys.env.getOrElse("KAFKA_OUTPUT_TOPIC", "trading-events-processed")
    val advisoryTopic = sys.env.getOrElse("KAFKA_ADVISORY_TOPIC", AdvisoryTopic)

    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId)
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass.getName)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass.getName)
    props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, classOf[EventTimeExtractor].getName)

    val streams = new KafkaStreams(buildTopology(inputTopic, outputTopic, advisoryTopic), props)
    println(s"[streams] appId=$appId bootstrap=$bootstrap in=$inputTopic out=$outputTopic adv=$advisoryTopic")

    val latch = new CountDownLatch(1)
    Runtime.getRuntime.addShutdownHook(new Thread("streams-shutdown") {
      override def run(): Unit =
        streams.close()
        latch.countDown()
    })
    streams.start()
    println("[streams] running (Ctrl-C to stop)")
    latch.await()

  final class EventTimeExtractor extends org.apache.kafka.streams.processor.TimestampExtractor:
    override def extract(
        record: org.apache.kafka.clients.consumer.ConsumerRecord[Object, Object],
        previousTimestamp: Long
    ): Long =
      val value = record.value().toString
      try
        val obj = ujson.read(value)
        val ts = obj("timestamp").str
        timestampToEpochMillis(ts)
      catch
        case _: Exception =>
          if previousTimestamp != 0L then previousTimestamp else 0L

