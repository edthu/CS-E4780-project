import java.util.Properties
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.streams.{StreamsConfig, TopologyTestDriver}
import scala.jdk.CollectionConverters.*

/** End-to-end topology test with no live broker: pipes records through the real
  * `buildTopology` (produce -> streams -> consume) and asserts one result per
  * closed window, crossover-only advisories, symbol deduplication, and one
  * queryable history point per closed window.
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
      StreamsApp.buildTopology("trading-events", "window-results", "advisories", "symbols"),
      props
    )
    try body(driver)
    finally driver.close()

  test("closed windows emit EMA results and crossover advisories"):
    withDriver { driver =>
      val in = driver.createInputTopic("trading-events", new StringSerializer, new StringSerializer)
      val windowResultsOut =
        driver.createOutputTopic("window-results", new StringDeserializer, new StringDeserializer)
      val advisoryOut = driver.createOutputTopic("advisories", new StringDeserializer, new StringDeserializer)

      // Three windows: [09:00,09:05), [09:05,09:10), [09:10,09:15). The third
      // record advances stream time far enough to close the first two.
      in.pipeInput("RDSA.NL", event("RDSA.NL", 42.5, "2021-11-08T09:00:00.000"))
      in.pipeInput("RDSA.NL", event("RDSA.NL", 50.0, "2021-11-08T09:06:00.000"))
      in.pipeInput("RDSA.NL", event("RDSA.NL", 40.0, "2021-11-08T09:11:00.000"))

      val firstWindow = windowResultsOut.readKeyValue()
      assertEquals(firstWindow.key, "RDSA.NL")
      val firstResult = ujson.read(firstWindow.value)
      assertEquals(firstResult("symbol").str, "RDSA.NL")
      assertEquals(firstResult("close").num, 42.5)
      assert(firstResult("ema38").num > 0)
      assert(firstResult("ema100").num > 0)
      assertEquals(firstResult("signal").str, "BUY")

      // With EMA_{s,w0} = 0 the first window already crosses (EMA38 = 2c/39 >
      // EMA100 = 2c/101 against 0 <= 0), so the only crossover is window 1.
      // Window 2 keeps EMA38 above EMA100, so it emits nothing.
      val advisoryRecord = advisoryOut.readKeyValue()
      assertEquals(advisoryRecord.key, "RDSA.NL")
      val advisoryObj = ujson.read(advisoryRecord.value)
      assertEquals(advisoryObj("signal").str, "BUY")
      assertEquals(advisoryObj("close").num, 42.5)
      assert(advisoryObj("ema38").num > advisoryObj("ema100").num)

      val secondWindow = windowResultsOut.readKeyValue()
      val secondResult = ujson.read(secondWindow.value)
      assertEquals(secondWindow.key, "RDSA.NL")
      assertEquals(secondResult("close").num, 50.0)
      assertEquals(secondResult("signal"), ujson.Null)
      assert(windowResultsOut.isEmpty, "the still-open third window must not emit")
      assert(advisoryOut.isEmpty, "no extra advisory records expected")
    }

  test("suppression evaluates a window once, so EMA steps once per closed window"):
    withDriver { driver =>
      val in = driver.createInputTopic("trading-events", new StringSerializer, new StringSerializer)
      val windowResults = driver.createOutputTopic("window-results", new StringDeserializer, new StringDeserializer)

      // Four ticks inside the first window; without suppression each one would
      // advance the EMA recurrence separately.
      in.pipeInput("AAA.FR", event("AAA.FR", 10.0, "2021-11-08T09:00:00.000"))
      in.pipeInput("AAA.FR", event("AAA.FR", 11.0, "2021-11-08T09:01:00.000"))
      in.pipeInput("AAA.FR", event("AAA.FR", 12.0, "2021-11-08T09:02:00.000"))
      in.pipeInput("AAA.FR", event("AAA.FR", 20.0, "2021-11-08T09:04:00.000"))
      in.pipeInput("AAA.FR", event("AAA.FR", 30.0, "2021-11-08T09:07:00.000"))
      in.pipeInput("AAA.FR", event("AAA.FR", 25.0, "2021-11-08T09:12:00.000"))

      val emittedResults = windowResults.readKeyValuesToList().asScala.toVector
      assertEquals(emittedResults.size, 2)
      assertEquals(ujson.read(emittedResults(0).value)("close").num, 20.0)
      assertEquals(ujson.read(emittedResults(1).value)("close").num, 30.0)

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

      // EMA_{s,w0} = 0, so the first window is the close weighted against zero.
      val fast1 = 20.0 * 2.0 / 39.0
      val slow1 = 20.0 * 2.0 / 101.0
      assert(Math.abs(points(0)("ema38").num - fast1) < 1e-12)
      assert(Math.abs(points(0)("ema100").num - slow1) < 1e-12)

      // Exactly one EMA step per closed window, from the previous window's EMA.
      val fast2 = (2.0 / 39.0) * 30.0 + (1.0 - 2.0 / 39.0) * fast1
      val slow2 = (2.0 / 101.0) * 30.0 + (1.0 - 2.0 / 101.0) * slow1
      assert(Math.abs(points(1)("ema38").num - fast2) < 1e-12)
      assert(Math.abs(points(1)("ema100").num - slow2) < 1e-12)

      // The first-window crossover is recorded alongside the EMA values; window
      // 2 stays above, so it carries no signal.
      assertEquals(ujson.read(emittedResults(0).value)("signal").str, "BUY")
      assertEquals(ujson.read(emittedResults(1).value)("signal"), ujson.Null)
      assertEquals(points(0)("signal").str, "BUY")
      assertEquals(points(1)("signal"), ujson.Null)
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

  test("processes events across multiple days carrying state, deduplicating symbols, and retaining history"):
    withDriver { driver =>
      val in = driver.createInputTopic("trading-events", new StringSerializer, new StringSerializer)
      val windowResultsOut =
        driver.createOutputTopic("window-results", new StringDeserializer, new StringDeserializer)
      val advisoryOut = driver.createOutputTopic("advisories", new StringDeserializer, new StringDeserializer)
      val symbolsOut = driver.createOutputTopic("symbols", new StringDeserializer, new StringDeserializer)

      // Day 1 (Nov 8): two windows for RDSA.NL ([09:00, 09:05) and [09:05, 09:10))
      in.pipeInput("RDSA.NL", event("RDSA.NL", 100.0, "2021-11-08T09:00:00.000"))
      in.pipeInput("RDSA.NL", event("RDSA.NL", 102.0, "2021-11-08T09:06:00.000"))
      // End of day 1: open a window at 17:25
      in.pipeInput("RDSA.NL", event("RDSA.NL", 105.0, "2021-11-08T17:25:00.000"))

      // Day 2 (Nov 9): morning event at 09:00 advances stream time past Nov 8 17:30, closing Day 1's last window
      in.pipeInput("RDSA.NL", event("RDSA.NL", 110.0, "2021-11-09T09:00:00.000"))
      in.pipeInput("RDSA.NL", event("RDSA.NL", 112.0, "2021-11-09T09:06:00.000"))

      // Day 5 (Nov 12): morning event advances stream time across mid-week gap to Friday
      in.pipeInput("RDSA.NL", event("RDSA.NL", 120.0, "2021-11-12T09:00:00.000"))
      in.pipeInput("RDSA.NL", event("RDSA.NL", 125.0, "2021-11-12T09:06:00.000"))

      // Pushing stream time past Friday 09:10
      in.pipeInput("RDSA.NL", event("RDSA.NL", 125.0, "2021-11-12T09:11:00.000"))

      // Check symbol registry: RDSA.NL emitted only once despite events across multiple days
      val symbols = symbolsOut.readKeyValuesToList().asScala.map(_.key).toVector
      assertEquals(symbols, Vector("RDSA.NL"))

      // Check window results: all 7 windows closed across the week
      val results = windowResultsOut.readKeyValuesToList().asScala.toVector
      assertEquals(results.size, 7)

      val store = driver.getWindowStore[String, String](StreamsApp.HistoryStore)
      val points =
        val it = store.fetch("RDSA.NL", java.time.Instant.ofEpochMilli(0L), java.time.Instant.parse("2022-01-01T00:00:00Z"))
        try it.asScala.map(kv => ujson.read(kv.value)).toVector
        finally it.close()

      // Full week history points are retained
      assertEquals(points.size, 7)
      // First point on Day 1 (Nov 8) is still present and valid
      assertEquals(points.head("close").num, 100.0)
      // Last point on Day 5 (Nov 12) is also present
      assertEquals(points.last("close").num, 125.0)

      // Verify that EMA from Day 1 carried over smoothly into Day 2
      val day1LastEmaFast = points(2)("ema38").num
      val day1LastEmaSlow = points(2)("ema100").num
      val day2FirstClose = 110.0
      val expectedDay2FirstFast = (2.0 / 39.0) * day2FirstClose + (1.0 - 2.0 / 39.0) * day1LastEmaFast
      val expectedDay2FirstSlow = (2.0 / 101.0) * day2FirstClose + (1.0 - 2.0 / 101.0) * day1LastEmaSlow
      assert(Math.abs(points(3)("ema38").num - expectedDay2FirstFast) < 1e-12)
      assert(Math.abs(points(3)("ema100").num - expectedDay2FirstSlow) < 1e-12)
    }
