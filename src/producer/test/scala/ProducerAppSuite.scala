import java.nio.charset.StandardCharsets
import java.nio.file.Files
import scala.jdk.CollectionConverters.*

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

  private def millis(timestamp: String): Long = ReplayPacer.epochMillis(timestamp)

  test("replay range is half open and blank bounds mean unset"):
    val range = ReplayRange(ReplayRange.parseBound("2021-11-11T11:30:00"), ReplayRange.parseBound("2021-11-11T12:30"))

    assert(!range.contains(millis("2021-11-11T11:29:59.999")))
    assert(range.contains(millis("2021-11-11T11:30:00.000")))
    assert(range.contains(millis("2021-11-11T12:29:59.999")))
    assert(!range.contains(millis("2021-11-11T12:30:00.000")))
    assertEquals(ReplayRange.parseBound("  "), None)
    assert(ReplayRange(None, None).isUnbounded)

  test("replay range rejects an empty interval"):
    intercept[IllegalArgumentException](ReplayRange(Some(10L), Some(10L)))

  test("selects only the files that can overlap the replay range"):
    val path = java.nio.file.Paths.get(_: String)
    val files = Vector(
      path("08.csv") -> millis("2021-11-08T07:00:00"),
      path("09.csv") -> millis("2021-11-09T07:00:00"),
      path("10.csv") -> millis("2021-11-10T07:00:00"),
      path("11.csv") -> millis("2021-11-11T07:00:00")
    )
    val within = ReplayRange(Some(millis("2021-11-09T15:00:00")), Some(millis("2021-11-09T18:00:00")))
    val spanning = ReplayRange(Some(millis("2021-11-09T15:00:00")), Some(millis("2021-11-10T08:00:00")))

    assertEquals(ReplayRange.selectFiles(files, within), Vector(path("09.csv")))
    assertEquals(ReplayRange.selectFiles(files, spanning), Vector(path("09.csv"), path("10.csv")))
    assertEquals(ReplayRange.selectFiles(files, ReplayRange(None, None)), files.map(_._1))

  test("end detection ignores isolated future timestamps"):
    val end = millis("2021-11-09T18:00:00")
    val detector = new ReplayEndDetector(ReplayRange(None, Some(end)))
    val past = end + ReplayRange.StopMarginMs

    (1 until ReplayRange.StopAfterEvents).foreach(_ => assert(!detector.observe(past)))
    assert(!detector.observe(end))
    (1 until ReplayRange.StopAfterEvents).foreach(_ => assert(!detector.observe(past)))
    assert(detector.observe(past))

  test("probe symbols cover every partition evenly"):
    val symbols = ProbeInjector.symbolsFor(6, 2)
    val perPartition = symbols.groupBy(ProbeInjector.partitionOf(_, 6)).view.mapValues(_.size).toMap

    assertEquals(symbols.distinct.size, 12)
    assertEquals(perPartition, (0 until 6).map(_ -> 2).toMap)

  test("probe rounds fire at the start and report the window each boundary closes"):
    val injector = new ProbeInjector(Vector("PROBE0000.T"))
    val first = millis("2021-11-11T11:31:00")
    val firstWindow = ProbeInjector.windowStart(first)

    assertEquals(injector.observe(first), Some(None))
    assertEquals(injector.observe(millis("2021-11-11T11:34:59.999")), None)
    assertEquals(injector.observe(millis("2021-11-11T11:35:00")), Some(Some(firstWindow)))
    assertEquals(injector.observe(millis("2021-11-11T11:34:00")), None) // out of order, no new round
    assertEquals(injector.observe(millis("2021-11-11T11:50:00")), Some(Some(firstWindow + ProbeInjector.WindowMs)))

  test("seek starts at the first row near the range and keeps its header"):
    val file = Files.createTempFile("producer-seek", ".csv")
    Files.writeString(file, """# comment
ID,SecType,Date,Time,Last
A.T,E,11-11-2021,09:00:00.000,1
A.T,E,11-11-2021,11:24:59.999,2
,,,,
A.T,E,11-11-2021,11:25:00.000,3
A.T,E,11-11-2021,11:00:00.000,4
A.T,E,11-11-2021,11:30:00.000,5
""", StandardCharsets.UTF_8)
    val parser = CsvSeek.open(file, ReplayRange(Some(millis("2021-11-11T11:30:00")), None), confirmRows = 1)
    try
      val lasts = parser.iterator().asScala.map(_.get("Last")).toList
      assertEquals(lasts, List("3", "4", "5"))
    finally parser.close()

  test("seek is not stopped by a stray late row before the range"):
    val file = Files.createTempFile("producer-seek-stray", ".csv")
    Files.writeString(file, """ID,SecType,Date,Time,Last
A.T,E,11-11-2021,23:00:00.000,1
A.T,E,11-11-2021,09:00:00.000,2
A.T,E,11-11-2021,11:26:00.000,3
,,,,
A.T,E,11-11-2021,11:27:00.000,4
A.T,E,11-11-2021,11:28:00.000,5
""", StandardCharsets.UTF_8)
    val parser = CsvSeek.open(file, ReplayRange(Some(millis("2021-11-11T11:30:00")), None), confirmRows = 2)
    try
      val lasts = parser.iterator().asScala.map(_.get("Last")).toList
      assertEquals(lasts, List("3", "", "4", "5"))
    finally parser.close()

  test("seek without a start reads the whole file"):
    val file = csv(Files.createTempFile("producer-seek-all", ".csv"), "08-11-2021", "09:00:00.000")
    val parser = CsvSeek.open(file, ReplayRange(None, None))
    try assertEquals(parser.iterator().asScala.size, 1)
    finally parser.close()
