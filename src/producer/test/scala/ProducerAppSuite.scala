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

  test("skips CSV files without valid price events instead of failing"):
    val directory = Files.createTempDirectory("producer-skip-test")
    val trading = csv(directory.resolve("day-01.csv"), "08-11-2021", "09:00:00.000")
    val weekend = Files.writeString(directory.resolve("day-07.csv"), header + "AAA,E,,,\n", StandardCharsets.UTF_8)

    assertEquals(ProducerApp.orderCsvFiles(Seq(weekend, trading)), Vector(trading))

  test("orders full week of daily CSVs (08-11 to 14-11) chronologically and skips weekend days"):
    val directory = Files.createTempDirectory("producer-week-test")
    val day08 = csv(directory.resolve("debs2022-gc-trading-day-08-11-21.csv"), "08-11-2021", "08:00:00.000")
    val day09 = csv(directory.resolve("debs2022-gc-trading-day-09-11-21.csv"), "09-11-2021", "08:00:00.000")
    val day10 = csv(directory.resolve("debs2022-gc-trading-day-10-11-21.csv"), "10-11-2021", "08:00:00.000")
    val day11 = csv(directory.resolve("debs2022-gc-trading-day-11-11-21.csv"), "11-11-2021", "08:00:00.000")
    val day12 = csv(directory.resolve("debs2022-gc-trading-day-12-11-21.csv"), "12-11-2021", "08:00:00.000")
    val day13 = Files.writeString(directory.resolve("debs2022-gc-trading-day-13-11-21.csv"), header + "AAA,E,0.0,02:00:00.000,13-11-2021\n", StandardCharsets.UTF_8)
    val day14 = Files.writeString(directory.resolve("debs2022-gc-trading-day-14-11-21.csv"), header + "AAA,E,,,,\n", StandardCharsets.UTF_8)

    val unordered = Seq(day12, day14, day09, day13, day08, day11, day10)
    val ordered = ProducerApp.orderCsvFiles(unordered)
    assertEquals(ordered, Vector(day08, day09, day10, day11, day12))

  test("timestamp pacing scales elapsed event time and permits unpaced replay"):
    val start = "2021-11-08T09:00:00.000"
    val tenSecondsLater = "2021-11-08T09:00:10.000"

    assertEquals(ReplayPacer.delayNanos(start, tenSecondsLater, 100.0), 100000000L)
    assertEquals(ReplayPacer.delayNanos(start, tenSecondsLater, 1.0), 10000000000L)
    assertEquals(ReplayPacer.delayNanos(start, tenSecondsLater, 0.0), 0L)