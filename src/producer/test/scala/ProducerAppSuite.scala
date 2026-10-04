import java.nio.charset.StandardCharsets
import java.nio.file.Files

class ProducerAppSuite extends munit.FunSuite:
  private val header = "ID,SecType,Last,Trading time,Trading date\n"

  private def csv(path: java.nio.file.Path, date: String, time: String): java.nio.file.Path =
    Files.writeString(path, header + s"AAA,E,10,$time,$date\n", StandardCharsets.UTF_8)

  test("orders daily CSV files by their first valid event timestamp"):
    val directory = Files.createTempDirectory("producer-order-test")
    val later = csv(directory.resolve("day-02.csv"), "09-11-2021", "09:00:00.000")
    val earlier = csv(directory.resolve("day-01.csv"), "08-11-2021", "09:00:00.000")

    assertEquals(ProducerApp.orderCsvFiles(Seq(later, earlier)), Vector(earlier, later))

  test("timestamp pacing scales elapsed event time and permits unpaced replay"):
    val start = "2021-11-08T09:00:00.000"
    val tenSecondsLater = "2021-11-08T09:00:10.000"

    assertEquals(ReplayPacer.delayNanos(start, tenSecondsLater, 100.0), 100000000L)
    assertEquals(ReplayPacer.delayNanos(start, tenSecondsLater, 1.0), 10000000000L)
    assertEquals(ReplayPacer.delayNanos(start, tenSecondsLater, 0.0), 0L)