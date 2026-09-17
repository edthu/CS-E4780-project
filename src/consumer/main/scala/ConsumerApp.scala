import java.time.Duration
import java.util.Properties
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import scala.jdk.CollectionConverters.*

/** Test / demo consumer (the "fake consumer" from the plan.png TODO list).
  *
  * Two verbosity levels via `CONSUMER_LOG_LEVEL`:
  *   - `ALL`     — print every record as `key -> value`.
  *   - `SUMMARY` — periodic throughput stats only (default), mirroring the
  *                 ingestion app's rows/second progress logging.
  * The summary cadence is set by `CONSUMER_SUMMARY_INTERVAL_MS` (default 2000).
  */
object ConsumerApp:
  def main(args: Array[String]): Unit =
    val bootstrap = sys.env.getOrElse("KAFKA_BOOTSTRAP", "localhost:9092")
    val topic = sys.env.getOrElse("KAFKA_TOPIC", "trading-events-processed")
    val group = sys.env.getOrElse("KAFKA_GROUP", "trading-consumer")
    val level = sys.env.getOrElse("CONSUMER_LOG_LEVEL", "SUMMARY").toUpperCase
    val summaryIntervalMs = sys.env
      .get("CONSUMER_SUMMARY_INTERVAL_MS")
      .flatMap(v => scala.util.Try(v.toLong).toOption)
      .filter(_ > 0)
      .getOrElse(2000L)

    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, group)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(java.util.List.of(topic))
    println(s"[consumer] bootstrap=$bootstrap topic=$topic group=$group level=$level (Ctrl-C to stop)")

    @volatile var running = true
    sys.addShutdownHook {
      running = false
      consumer.wakeup()
    }

    val startedAt = System.nanoTime()
    var consumed = 0L
    var lastSummaryAt = startedAt
    var lastSummaryCount = 0L

    def rate(delta: Long, nanos: Long): Double =
      if nanos <= 0 then 0.0 else delta * 1e9 / nanos

    try
      while running do
        // Keep polling forever so the consumer survives quiet periods (the
        // producer between batches) and transient errors like a topic being
        // deleted/recreated by scripts/reset-topics.sh — new data resumes flow.
        try
          val records = consumer.poll(Duration.ofMillis(500))
          for record <- records.asScala do
            consumed += 1
            if level == "ALL" then println(s"${record.key()} -> ${record.value()}")
          if level == "SUMMARY" then
            val now = System.nanoTime()
            if now - lastSummaryAt >= summaryIntervalMs * 1000000L && consumed > lastSummaryCount then
              val intervalRps = rate(consumed - lastSummaryCount, now - lastSummaryAt)
              val avgRps = rate(consumed, now - startedAt)
              println(
                f"[consumer] consumed=$consumed recordsPerSecond=$intervalRps%.1f avgRecordsPerSecond=$avgRps%.1f"
              )
              lastSummaryAt = now
              lastSummaryCount = consumed
        catch
          case _: WakeupException => running = false
          case e: Exception if running =>
            System.err.println(s"[consumer] poll error (retrying): ${e.getMessage}")
            Thread.sleep(1000)
    finally
      val elapsedMillis = (System.nanoTime() - startedAt) / 1000000L
      val avgRps = rate(consumed, System.nanoTime() - startedAt)
      consumer.close()
      println(
        f"[consumer] closed consumed=$consumed elapsedMillis=$elapsedMillis avgRecordsPerSecond=$avgRps%.1f"
      )
