import java.net.InetAddress
import java.nio.charset.StandardCharsets
import java.time.{Duration, Instant, LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.Properties
import java.util.concurrent.CountDownLatch
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsBuilder, StreamsConfig, Topology}
import org.apache.kafka.streams.kstream.{Consumed, KStream, Materialized, Produced, Suppressed, TimeWindows, Transformer, TransformerSupplier, ValueMapper, Windowed}
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.{KeyValueStore, Stores, WindowStore}

/** Kafka Streams application.
  *
  * The stream enriches each input event with EMA state and emits advisories for
  * BUY/SELL crossovers. This follows the project assignment pattern: 5-minute tumbling windows over the
  * raw trading events, using the last price in each window as the closing value,
  * and then evaluating the EMA signals on the resulting window close.
  *
  * Three side outputs feed the rest of the system:
  *   - `trading-events-processed`: every event, enriched with per-tick EMAs.
  *   - `advisories`: one record per EMA crossover (Query 2).
  *   - `symbols`: a log-compacted registry of every symbol seen, for the UI.
  *
  * The `ema-history` window store is queryable, so the UI reads the Query 1 time
  * series straight out of Streams state via [[QueryService]] rather than
  * re-materialising it from a topic.
  */
object StreamsApp:
  val AdvisoryTopic = "advisories"
  val SymbolTopic = "symbols"
  val WindowSize = Duration.ofMinutes(5)
  val CestZone = ZoneId.of("Europe/Amsterdam")

  /** Queryable store holding one EMA point per symbol per closed window. */
  val HistoryStore = "ema-history"
  private val RecordStateStore = "record-ema-state"
  private val WindowStateStore = "window-ema-state"
  private val SymbolSeenStore = "symbol-seen"

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

  /** One point on the chart the UI draws: both EMAs plus the window close, and
    * the advisory (if any) that fired on this very window. Keeping the signal in
    * the same record means the UI needs a single interactive query to draw both
    * the lines and the BUY/SELL markers.
    */
  def renderHistoryPoint(
      windowStart: Double,
      windowEnd: Double,
      close: Double,
      emaFast: Double,
      emaSlow: Double,
      signal: Option[String]
  ): String =
    ujson.Obj(
      "windowStart" -> windowStart,
      "windowEnd" -> windowEnd,
      "close" -> close,
      "ema38" -> emaFast,
      "ema100" -> emaSlow,
      "signal" -> signal.map(ujson.Str(_)).getOrElse(ujson.Null)
    ).render()

  /** Changelog overrides for the *windowed* stores.
    *
    * Kafka Streams gives window-store changelogs `cleanup.policy=compact,delete`
    * with `retention.ms` derived from the store's retention period. Those
    * records carry **event** timestamps, and this data set is a replay of
    * November 2021 -- every record is years older than any sane retention, so
    * the broker deletes the whole changelog almost as fast as it is written and
    * the store can never be restored. (Key-value store changelogs are
    * compact-only, which is why they survive.)
    *
    * `retention.ms=-1` disables time-based deletion; compaction still bounds the
    * topic to one record per (symbol, window).
    */
  private val WindowChangelogConfig: java.util.Map[String, String] =
    java.util.Map.of("retention.ms", "-1", "cleanup.policy", "compact,delete")

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

  /** Evaluates the EMA recurrence once per *closed* window and records the
    * result in the queryable history store. Only crossovers are forwarded
    * downstream, so the `advisories` topic stays a low-volume signal feed.
    */
  private final class WindowSignalTransformer extends Transformer[String, String, KeyValue[String, String]]:
    private var store: KeyValueStore[String, String] = _
    private var history: WindowStore[String, String] = _

    override def init(context: ProcessorContext): Unit =
      this.store = context.getStateStore(WindowStateStore).asInstanceOf[KeyValueStore[String, String]]
      this.history = context.getStateStore(HistoryStore).asInstanceOf[WindowStore[String, String]]

    override def transform(symbol: String, value: String): KeyValue[String, String] =
      val obj = ujson.read(value)
      val close = obj("lastClose").num
      val windowStart = obj("windowStart").num
      val windowEnd = obj("windowEnd").num
      val previousState = parseState(store.get(symbol))
      val nextFast = calculateEma(previousState.emaFast, close, 38)
      val nextSlow = calculateEma(previousState.emaSlow, close, 100)
      val signal = detectSignal(previousState.emaFast, previousState.emaSlow, nextFast, nextSlow)
      store.put(symbol, renderState(EmaState(nextFast, nextSlow)))

      // Every closed window becomes a chart point, crossover or not.
      history.put(
        symbol,
        renderHistoryPoint(windowStart, windowEnd, close, nextFast, nextSlow, signal),
        windowStart.toLong
      )

      signal match
        case Some(label) =>
          val advisory = ujson.Obj(
            "symbol" -> symbol,
            "signal" -> label,
            "ema38" -> nextFast,
            "ema100" -> nextSlow,
            "close" -> close,
            "windowStart" -> windowStart,
            "windowEnd" -> windowEnd
          )
          KeyValue.pair(symbol, advisory.render())
        case None => null

    override def close(): Unit = ()

  /** Emits a symbol exactly once, the first time it is ever observed. The
    * dedupe store turns a 289M-event stream into at most one registry record
    * per symbol, which is what keeps the compacted `symbols` topic cheap to
    * replay from the UI.
    */
  private final class SymbolRegistryTransformer extends Transformer[String, String, KeyValue[String, String]]:
    private var seen: KeyValueStore[String, String] = _
    private var context: ProcessorContext = _

    override def init(context: ProcessorContext): Unit =
      this.context = context
      this.seen = context.getStateStore(SymbolSeenStore).asInstanceOf[KeyValueStore[String, String]]

    override def transform(key: String, value: String): KeyValue[String, String] =
      val obj = ujson.read(value)
      val symbol = obj("symbol").str
      if seen.get(symbol) != null then null
      else
        seen.put(symbol, "1")
        val securityType =
          if obj.obj.contains("securityType") then obj("securityType").str else ""
        val record = ujson.Obj(
          "symbol" -> symbol,
          "securityType" -> securityType,
          "firstSeen" -> context.timestamp().toDouble
        )
        KeyValue.pair(symbol, record.render())

    override def close(): Unit = ()

  def emittedProcessedRecord(value: String, emaFast: Double, emaSlow: Double): String =
    enrichProcessed(value, emaFast, emaSlow)

  def buildTopology(
      inputTopic: String,
      outputTopic: String,
      advisoryTopic: String = AdvisoryTopic,
      symbolTopic: String = SymbolTopic,
      historyRetention: Duration = Duration.ofDays(8)
  ): Topology =
    val builder = new StreamsBuilder()
    val stringSerde = Serdes.String()
    builder.addStateStore(
      Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(RecordStateStore),
        Serdes.String(),
        Serdes.String()
      )
    )
    builder.addStateStore(
      Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(WindowStateStore),
        Serdes.String(),
        Serdes.String()
      )
    )
    builder.addStateStore(
      Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(SymbolSeenStore),
        Serdes.String(),
        Serdes.String()
      )
    )
    // Retention must cover the span the UI can ask for; the window size matches
    // the assignment's 5-minute tumbling windows.
    builder.addStateStore(
      Stores
        .windowStoreBuilder(
          Stores.persistentWindowStore(HistoryStore, historyRetention, WindowSize, false),
          Serdes.String(),
          Serdes.String()
        )
        .withLoggingEnabled(WindowChangelogConfig)
    )

    val source: KStream[String, String] =
      builder.stream(inputTopic, Consumed.`with`(stringSerde, stringSerde))

    val processedSupplier = new TransformerSupplier[String, String, KeyValue[String, String]] {
      override def get(): Transformer[String, String, KeyValue[String, String]] =
        new Transformer[String, String, KeyValue[String, String]] {
          private var store: KeyValueStore[String, String] = _

          override def init(context: ProcessorContext): Unit =
            this.store = context.getStateStore(RecordStateStore).asInstanceOf[KeyValueStore[String, String]]

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

    val enrichedProcessed = source.transform(processedSupplier, RecordStateStore)

    enrichedProcessed.to(outputTopic, Produced.`with`(stringSerde, stringSerde))

    val symbolSupplier = new TransformerSupplier[String, String, KeyValue[String, String]] {
      override def get(): Transformer[String, String, KeyValue[String, String]] =
        new SymbolRegistryTransformer()
    }

    source
      .transform(symbolSupplier, SymbolSeenStore)
      .filter((_: String, v: String) => v != null)
      .to(symbolTopic, Produced.`with`(stringSerde, stringSerde))

    val windows = source
      .groupByKey()
      .windowedBy(TimeWindows.ofSizeWithNoGrace(WindowSize))
      .aggregate(
        () => WindowAccumulator(),
        (_, raw: String, current: WindowAccumulator) => current.update(raw),
        Materialized.`with`(stringSerde, windowAccumulatorSerde).withLoggingEnabled(WindowChangelogConfig)
      )
      // The assignment evaluates window w_i only once w_{i+1} starts. Without
      // this, `toStream()` would emit a partial aggregate per input event and
      // the EMA recurrence would be applied many times inside a single window.
      .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
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

    val advisories = windows.transform(advisoriesSupplier, WindowStateStore, HistoryStore)
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
    val symbolTopic = sys.env.getOrElse("KAFKA_SYMBOL_TOPIC", SymbolTopic)
    val queryPort = sys.env.getOrElse("STREAMS_QUERY_PORT", "7070").toInt
    val retentionDays = sys.env.getOrElse("EMA_HISTORY_RETENTION_DAYS", "8").toLong
    val standbys = sys.env.getOrElse("STREAMS_STANDBY_REPLICAS", "0").toInt
    // Kafka Streams takes an exclusive lock on its state directory, so replicas
    // sharing one volume must not share a path. Keying the directory on the
    // container hostname lets `--scale streams=N` work against a single volume;
    // if an instance comes back with a new hostname it simply restores its
    // stores from the changelog topics, which is what makes them fault-tolerant
    // in the first place.
    val stateDir = sys.env
      .get("STREAMS_STATE_DIR")
      .filter(_.nonEmpty)
      .map(base => s"$base/${InetAddress.getLocalHost.getHostName}")

    // Every replica must advertise a *distinct* address, otherwise interactive
    // query routing sends every lookup to whichever instance registered last.
    // Defaulting to the container's own IP gives each replica under
    // `--scale streams=N` a unique, directly routable address without relying on
    // DNS resolution of container hostnames.
    val advertisedHost = sys.env
      .get("STREAMS_APPLICATION_SERVER")
      .map(_.trim)
      .filter(_.nonEmpty)
      .getOrElse(s"${InetAddress.getLocalHost.getHostAddress}:$queryPort")

    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId)
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass.getName)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass.getName)
    props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, classOf[EventTimeExtractor].getName)
    props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, advertisedHost)
    props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, Integer.valueOf(standbys))
    stateDir.foreach(dir => props.put(StreamsConfig.STATE_DIR_CONFIG, dir))

    val topology =
      buildTopology(inputTopic, outputTopic, advisoryTopic, symbolTopic, Duration.ofDays(retentionDays))
    val streams = new KafkaStreams(topology, props)
    println(
      s"[streams] appId=$appId bootstrap=$bootstrap in=$inputTopic out=$outputTopic " +
        s"adv=$advisoryTopic symbols=$symbolTopic server=$advertisedHost"
    )

    val queryService = new QueryService(streams, advertisedHost, queryPort)

    val latch = new CountDownLatch(1)
    Runtime.getRuntime.addShutdownHook(new Thread("streams-shutdown") {
      override def run(): Unit =
        queryService.stop()
        streams.close()
        latch.countDown()
    })
    streams.start()
    queryService.start()
    println(s"[streams] running, interactive queries on :$queryPort (Ctrl-C to stop)")
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
