import java.util.Properties
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.streams.{StreamsConfig, TopologyTestDriver}

/** End-to-end topology test with no live broker: pipes a record through the real
  * `buildTopology` (produce -> streams -> consume) and asserts the key survives
  * while the processed topic includes EMA values and the advisory topic emits
  * trading signals. Mirrors the docker demo's success criteria.
  */
class StreamsTopologySuite extends munit.FunSuite:
  test("record flows in -> processed topic includes EMA values and advisory signals"):
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")

    val driver = new TopologyTestDriver(
      StreamsApp.buildTopology("trading-events", "trading-events-processed", "advisories"),
      props
    )
    try
      val in = driver.createInputTopic("trading-events", new StringSerializer, new StringSerializer)
      val processedOut = driver.createOutputTopic("trading-events-processed", new StringDeserializer, new StringDeserializer)
      val advisoryOut = driver.createOutputTopic("advisories", new StringDeserializer, new StringDeserializer)

      val first = """{"symbol":"RDSA.NL","securityType":"E","price":42.5,"timestamp":"2021-11-08T09:00:00.000"}"""
      val second = """{"symbol":"RDSA.NL","securityType":"E","price":50.0,"timestamp":"2021-11-08T09:06:00.000"}"""

      in.pipeInput("RDSA.NL", first)
      in.pipeInput("RDSA.NL", second)

      val processedRecord = processedOut.readKeyValue()
      assertEquals(processedRecord.key, "RDSA.NL")
      val processedObj = ujson.read(processedRecord.value)
      assert(!processedObj.obj.contains("processedBy"), "processedBy should not be present")
      assertEquals(processedObj("symbol").str, "RDSA.NL")
      assertEquals(processedObj("price").num, 42.5)
      assert(processedObj("ema38").num > 0)
      assert(processedObj("ema100").num > 0)

      val advisoryRecord = advisoryOut.readKeyValue()
      assertEquals(advisoryRecord.key, "RDSA.NL")
      val advisoryObj = ujson.read(advisoryRecord.value)
      assert(advisoryObj("signal").str == "BUY" || advisoryObj("signal").str == "SELL")
      assert(advisoryObj("ema38").num > 0)
      assert(advisoryObj("ema100").num > 0)

      val secondProcessedRecord = processedOut.readKeyValue()
      assertEquals(secondProcessedRecord.key, "RDSA.NL")
      assertEquals(ujson.read(secondProcessedRecord.value)("price").num, 50.0)
      assert(processedOut.isEmpty, "no extra processed records expected")
      assert(advisoryOut.isEmpty, "no extra advisory records expected")
    finally driver.close()
