import java.util.Properties
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.streams.{StreamsConfig, TopologyTestDriver}

/** End-to-end topology test with no live broker: pipes a record through the real
  * `buildTopology` (produce -> streams -> consume) and asserts the key survives
  * and the marker is applied. Mirrors the docker demo's success criteria.
  */
class StreamsTopologySuite extends munit.FunSuite:
  test("record flows in -> topology -> out, keyed by symbol, with marker"):
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")

    val driver = new TopologyTestDriver(StreamsApp.buildTopology("trading-events", "trading-events-processed"), props)
    try
      val in = driver.createInputTopic("trading-events", new StringSerializer, new StringSerializer)
      val out = driver.createOutputTopic("trading-events-processed", new StringDeserializer, new StringDeserializer)

      val value = """{"symbol":"RDSA.NL","securityType":"E","price":42.5,"timestamp":"2021-11-08T09:00:00.000"}"""
      in.pipeInput("RDSA.NL", value)

      val record = out.readKeyValue()
      assertEquals(record.key, "RDSA.NL")
      val obj = ujson.read(record.value)
      assertEquals(obj("processedBy").str, "processed by kafka streams")
      assertEquals(obj("symbol").str, "RDSA.NL")
      assertEquals(obj("price").num, 42.5)
      assert(out.isEmpty, "exactly one output record expected")
    finally driver.close()
