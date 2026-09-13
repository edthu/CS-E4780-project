class CsvEventParserSuite extends munit.FunSuite:
  private val validRow = Map(
    "ID" -> "AAPL.ETR",
    "SecType" -> "E",
    "Last" -> "123.45",
    "Trading time" -> "01:01:07.000",
    "Trading date" -> "08-11-2021"
  )

  test("parses the five required source fields"):
    val result = CsvEventParser.parse(1, CsvEventParser.normalizeHeaders(validRow))
    assertEquals(
      result,
      ParseResult.Accepted(Event("AAPL.ETR", "E", 123.45, "2021-11-08T01:01:07.000"))
    )

  test("rejects rows with a missing required field"):
    val result = CsvEventParser.parse(2, CsvEventParser.normalizeHeaders(validRow - "Last"))
    assertEquals(result, ParseResult.Skipped(SkippedRow(2, "non-price event: missing last")))

  test("rejects malformed prices"):
    val result = CsvEventParser.parse(3, CsvEventParser.normalizeHeaders(validRow.updated("Last", "not-a-price")))
    assertEquals(result, ParseResult.Rejected(RejectedRow(3, "invalid price: not-a-price")))

  test("rejects non-positive prices"):
    val result = CsvEventParser.parse(4, CsvEventParser.normalizeHeaders(validRow.updated("Last", "0")))
    assertEquals(result, ParseResult.Rejected(RejectedRow(4, "invalid price: 0")))

  test("accepts timestamps with four fractional digits"):
    val result = CsvEventParser.parse(5, CsvEventParser.normalizeHeaders(validRow.updated("Trading time", "01:01:07.1234")))
    assert(result.isInstanceOf[ParseResult.Accepted])