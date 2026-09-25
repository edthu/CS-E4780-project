import java.util.Properties
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.streams.{StreamsConfig, TopologyTestDriver}
import scala.jdk.CollectionConverters.*

/** End-to-end topology test with no live broker: pipes records through the real
  * `buildTopology` (produce -> streams -> consume) and asserts the key survives
  * while the processed topic includes EMA values, the advisory topic emits
  * trading signals, the symbol registry stays deduplicated and the queryable
  * history store gets exactly one point per closed window.
  */
class StreamsTopologySuite extends munit.FunSuite:

  private def event(symbol: String, price: Double, timestamp: String): String =
    s"""{"symbol":"$symbol","securityType":"E","price":$price,"timestamp":"$timestamp"}"""

  private def withDriver[A](body: TopologyTestDriver => A): A =
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")
    // Without the real extractor every piped record would carry timestamp 0, so
    // stream time would never advance and suppressed windows would never close.
    props.put(
      StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
      classOf[StreamsApp.EventTimeExtractor].getName
    )
    val driver = new TopologyTestDriver(
      StreamsApp.buildTopology("trading-events", "trading-events-processed", "advisories", "symbols"),
      props
    )
    try body(driver)
    finally driver.close()

  test("record flows in -> processed topic includes EMA values and advisory signals"):
    withDriver { driver =>
      val in = driver.createInputTopic("trading-events", new StringSerializer, new StringSerializer)
      val processedOut =
        driver.createOutputTopic("trading-events-processed", new StringDeserializer, new StringDeserializer)
      val advisoryOut = driver.createOutputTopic("advisories", new StringDeserializer, new StringDeserializer)

      // Three windows: [09:00,09:05), [09:05,09:10), [09:10,09:15). The third
      // record advances stream time far enough to close the first two.
      in.pipeInput("RDSA.NL", event("RDSA.NL", 42.5, "2021-11-08T09:00:00.000"))
      in.pipeInput("RDSA.NL", event("RDSA.NL", 50.0, "2021-11-08T09:06:00.000"))
      in.pipeInput("RDSA.NL", event("RDSA.NL", 40.0, "2021-11-08T09:11:00.000"))

      val processedRecord = processedOut.readKeyValue()
      assertEquals(processedRecord.key, "RDSA.NL")
      val processedObj = ujson.read(processedRecord.value)
      assert(!processedObj.obj.contains("processedBy"), "processedBy should not be present")
      assertEquals(processedObj("symbol").str, "RDSA.NL")
      assertEquals(processedObj("price").num, 42.5)
      assert(processedObj("ema38").num > 0)
      assert(processedObj("ema100").num > 0)

      // The first window closes with EMA38 == EMA100 (both seeded from zero),
      // so the only crossover is the second window's rise to 50.0.
      val advisoryRecord = advisoryOut.readKeyValue()
      assertEquals(advisoryRecord.key, "RDSA.NL")
      val advisoryObj = ujson.read(advisoryRecord.value)
      assertEquals(advisoryObj("signal").str, "BUY")
      assertEquals(advisoryObj("close").num, 50.0)
      assert(advisoryObj("ema38").num > advisoryObj("ema100").num)

      val secondProcessedRecord = processedOut.readKeyValue()
      assertEquals(secondProcessedRecord.key, "RDSA.NL")
      assertEquals(ujson.read(secondProcessedRecord.value)("price").num, 50.0)
      assertEquals(ujson.read(processedOut.readKeyValue().value)("price").num, 40.0)
      assert(processedOut.isEmpty, "no extra processed records expected")
      assert(advisoryOut.isEmpty, "no extra advisory records expected")
    }

  test("suppression evaluates a window once, so EMA steps once per closed window"):
    withDriver { driver =>
      val in = driver.createInputTopic("trading-events", new StringSerializer, new StringSerializer)

      // Four ticks inside the first window; without suppression each one would
      // advance the EMA recurrence separately.
      in.pipeInput("AAA.FR", event("AAA.FR", 10.0, "2021-11-08T09:00:00.000"))
      in.pipeInput("AAA.FR", event("AAA.FR", 11.0, "2021-11-08T09:01:00.000"))
      in.pipeInput("AAA.FR", event("AAA.FR", 12.0, "2021-11-08T09:02:00.000"))
      in.pipeInput("AAA.FR", event("AAA.FR", 20.0, "2021-11-08T09:04:00.000"))
      in.pipeInput("AAA.FR", event("AAA.FR", 30.0, "2021-11-08T09:07:00.000"))
      in.pipeInput("AAA.FR", event("AAA.FR", 25.0, "2021-11-08T09:12:00.000"))

      val store = driver.getWindowStore[String, String](StreamsApp.HistoryStore)
      val points =
        val it = store.fetch("AAA.FR", java.time.Instant.ofEpochMilli(0L), java.time.Instant.parse("2022-01-01T00:00:00Z"))
        try it.asScala.map(kv => ujson.read(kv.value)).toVector
        finally it.close()

      // Two windows closed ([09:00,09:05) and [09:05,09:10)); the third is open.
      assertEquals(points.size, 2)

      // Window 1 closes at the *last* price in the window, not the last tick seen.
      assertEquals(points(0)("close").num, 20.0)
      assertEquals(points(1)("close").num, 30.0)

      // EMA seeded from zero collapses to the close on the first window.
      assertEquals(points(0)("ema38").num, 20.0)
      assertEquals(points(0)("ema100").num, 20.0)

      val expectedFast = StreamsApp.calculateEma(20.0, 30.0, 38)
      val expectedSlow = StreamsApp.calculateEma(20.0, 30.0, 100)
      assert(Math.abs(points(1)("ema38").num - expectedFast) < 1e-12)
      assert(Math.abs(points(1)("ema100").num - expectedSlow) < 1e-12)

      // The crossover on window 2 is recorded alongside the EMA values.
      assertEquals(points(0)("signal"), ujson.Null)
      assertEquals(points(1)("signal").str, "BUY")
    }

  test("symbol registry emits each symbol exactly once"):
    withDriver { driver =>
      val in = driver.createInputTopic("trading-events", new StringSerializer, new StringSerializer)
      val symbolsOut = driver.createOutputTopic("symbols", new StringDeserializer, new StringDeserializer)

      in.pipeInput("AAA.FR", event("AAA.FR", 10.0, "2021-11-08T09:00:00.000"))
      in.pipeInput("BBB.NL", event("BBB.NL", 20.0, "2021-11-08T09:00:01.000"))
      in.pipeInput("AAA.FR", event("AAA.FR", 11.0, "2021-11-08T09:00:02.000"))
      in.pipeInput("AAA.FR", event("AAA.FR", 12.0, "2021-11-08T09:06:00.000"))
      in.pipeInput("BBB.NL", event("BBB.NL", 21.0, "2021-11-08T09:06:01.000"))

      val records = symbolsOut.readKeyValuesToList().asScala.toVector
      assertEquals(records.map(_.key), Vector("AAA.FR", "BBB.NL"))

      val first = ujson.read(records.head.value)
      assertEquals(first("symbol").str, "AAA.FR")
      assertEquals(first("securityType").str, "E")
      assert(first("firstSeen").num > 0)

      assert(symbolsOut.isEmpty, "repeat sightings must not be re-emitted")
    }
