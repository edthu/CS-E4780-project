import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import org.apache.commons.csv.{CSVFormat, CSVParser}
import scala.jdk.CollectionConverters.*

final case class IngestionSummary(
    totalRows: Long,
    accepted: Long,
    skippedNonPrice: Long,
    rejected: Long,
    elapsedMillis: Long
):
  def rowsPerSecond: Double =
    if elapsedMillis == 0 then 0.0 else totalRows * 1000.0 / elapsedMillis

object IngestionApp:
  private val defaultInput = "/data/debs2022-gc-trading-day-08-11-21.csv"
  private val defaultOutput = "/output/events.ndjson"
  private val defaultProgressIntervalRows = 1000000L

  def main(args: Array[String]): Unit =
    val input = Paths.get(args.headOption.getOrElse(defaultInput))
    val output = Paths.get(args.drop(1).headOption.getOrElse(defaultOutput))
    val summary = ingest(input, output)
    println(
      s"totalRows=${summary.totalRows} accepted=${summary.accepted} " +
        s"skippedNonPrice=${summary.skippedNonPrice} rejected=${summary.rejected} " +
        f"elapsedMillis=${summary.elapsedMillis} rowsPerSecond=${summary.rowsPerSecond}%.2f"
    )

  def ingest(input: Path, output: Path): IngestionSummary =
    createParent(output)
    val progressIntervalRows = sys.env
      .get("INGESTION_PROGRESS_INTERVAL_ROWS")
      .flatMap(value => scala.util.Try(value.toLong).toOption)
      .filter(_ > 0)
      .getOrElse(defaultProgressIntervalRows)
    val inputSizeBytes = Files.size(input)
    println(
      s"[ingestion] starting input=$input size=${formatBytes(inputSizeBytes)} " +
        s"progressIntervalRows=$progressIntervalRows"
    )
    val format = CSVFormat.DEFAULT.builder()
      .setCommentMarker('#')
      .setHeader()
      .setSkipHeaderRecord(true)
      .build()
    var totalRows = 0L
    var accepted = 0L
    var skippedNonPrice = 0L
    var rejected = 0L
    val startedAt = System.nanoTime()
    val parser = CSVParser.parse(input, StandardCharsets.UTF_8, format)
    try
      val eventsWriter = Files.newBufferedWriter(output, StandardCharsets.UTF_8)
      try
        parser.iterator().asScala.foreach { record =>
          totalRows += 1
          val row = CsvEventParser.normalizeHeaders(record.toMap.asScala.toMap)
          CsvEventParser.parse(record.getRecordNumber, row) match
            case ParseResult.Accepted(event) =>
              eventsWriter.write(ujson.Obj(
                "symbol" -> event.symbol,
                "securityType" -> event.securityType,
                "price" -> event.price,
                "timestamp" -> event.timestamp
              ).render())
              eventsWriter.newLine()
              accepted += 1
            case ParseResult.Skipped(_) =>
              skippedNonPrice += 1
            case ParseResult.Rejected(_) =>
              rejected += 1
          if totalRows % progressIntervalRows == 0 then
            logProgress(totalRows, accepted, skippedNonPrice, rejected, startedAt)
        }
      finally
        eventsWriter.close()
    finally parser.close()
    val elapsedMillis = (System.nanoTime() - startedAt) / 1000000L
    val summary = IngestionSummary(totalRows, accepted, skippedNonPrice, rejected, elapsedMillis)
    logProgress(totalRows, accepted, skippedNonPrice, rejected, startedAt)
    println(s"[ingestion] completed elapsedMillis=$elapsedMillis")
    summary

  private def createParent(path: Path): Unit =
    Option(path.getParent).foreach(Files.createDirectories(_))

  private def logProgress(
      totalRows: Long,
      accepted: Long,
      skippedNonPrice: Long,
      rejected: Long,
      startedAt: Long
  ): Unit =
    val elapsedMillis = (System.nanoTime() - startedAt) / 1000000L
    val rowsPerSecond = if elapsedMillis == 0 then 0.0 else totalRows * 1000.0 / elapsedMillis
    println(
      f"[ingestion] progress rows=$totalRows accepted=$accepted " +
        f"skippedNonPrice=$skippedNonPrice rejected=$rejected " +
        f"elapsedMillis=$elapsedMillis rowsPerSecond=$rowsPerSecond%.2f"
    )

  private def formatBytes(bytes: Long): String =
    if bytes >= 1024L * 1024L * 1024L then f"${bytes.toDouble / (1024 * 1024 * 1024)}%.2f GiB"
    else if bytes >= 1024L * 1024L then f"${bytes.toDouble / (1024 * 1024)}%.2f MiB"
    else if bytes >= 1024L then f"${bytes.toDouble / 1024}%.2f KiB"
    else s"${bytes} B"