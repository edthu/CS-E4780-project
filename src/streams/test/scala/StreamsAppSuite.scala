class StreamsAppSuite extends munit.FunSuite:
  test("addMarker adds processedBy and preserves original fields"):
    val input =
      """{"symbol":"GOOD","securityType":"E","price":123.45,"timestamp":"2021-11-08T01:01:07.000"}"""

    val obj = ujson.read(StreamsApp.addMarker(input))

    assertEquals(obj("processedBy").str, "processed by kafka streams")
    assertEquals(obj("symbol").str, "GOOD")
    assertEquals(obj("securityType").str, "E")
    assertEquals(obj("price").num, 123.45)
    assertEquals(obj("timestamp").str, "2021-11-08T01:01:07.000")
