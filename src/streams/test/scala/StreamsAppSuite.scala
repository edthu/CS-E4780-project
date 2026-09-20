class StreamsAppSuite extends munit.FunSuite:
  test("enrichProcessed adds EMA fields to the event payload"):
    val input =
      """{"symbol":"GOOD","securityType":"E","price":123.45,"timestamp":"2021-11-08T01:01:07.000"}"""

    val obj = ujson.read(StreamsApp.enrichProcessed(input, 124.0, 123.0))

    assertEquals(obj("symbol").str, "GOOD")
    assertEquals(obj("securityType").str, "E")
    assertEquals(obj("price").num, 123.45)
    assertEquals(obj("timestamp").str, "2021-11-08T01:01:07.000")
    assertEquals(obj("ema38").num, 124.0)
    assertEquals(obj("ema100").num, 123.0)

  test("calculateEma uses the EMA smoothing formula"):
    val first = StreamsApp.calculateEma(0.0, 100.0, 38)
    assertEquals(first, 100.0)

    val next = StreamsApp.calculateEma(100.0, 110.0, 38)
    val alpha = 2.0 / (38.0 + 1.0)
    val expected = alpha * 110.0 + (1.0 - alpha) * 100.0
    assert(Math.abs(next - expected) < 1e-12)

  test("detectSignal emits BUY and SELL purely on EMA crossovers"):
    assertEquals(StreamsApp.detectSignal(99.0, 100.0, 101.0, 99.0), Some("BUY"))
    assertEquals(StreamsApp.detectSignal(101.0, 99.0, 100.0, 101.0), Some("SELL"))
    assertEquals(StreamsApp.detectSignal(101.0, 99.0, 101.5, 98.0), None)
