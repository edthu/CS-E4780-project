import java.nio.charset.StandardCharsets
import java.nio.file.Files

class IngestionAppSuite extends munit.FunSuite:
  test("streams source rows into event output"):
    val directory = Files.createTempDirectory("ingestion-test")
    val input = directory.resolve("input.csv")
    val output = directory.resolve("events.ndjson")
    Files.writeString(input, """# license metadata
# more metadata
ID,SecType,Last,Trading time,Trading date
GOOD,E,123.45,01:01:07.000,08-11-2021
BAD,E,,01:01:08.000,08-11-2021
""", StandardCharsets.UTF_8)

    val summary = IngestionApp.ingest(input, output)

    assertEquals(summary.totalRows, 2L)
    assertEquals(summary.accepted, 1L)
    assertEquals(summary.skippedNonPrice, 1L)
    assertEquals(summary.rejected, 0L)
    val outputText = Files.readString(output)
    assert(outputText.contains("\"symbol\":\"GOOD\""))
    assert(outputText.contains("\"securityType\":\"E\""))
    assert(outputText.contains("\"timestamp\":\"2021-11-08T01:01:07.000\""))