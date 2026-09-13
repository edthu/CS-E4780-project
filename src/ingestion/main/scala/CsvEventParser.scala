import java.time.LocalDateTime
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.temporal.ChronoField
import scala.util.Try

final case class RejectedRow(rowNumber: Long, reason: String)
final case class SkippedRow(rowNumber: Long, reason: String)

enum ParseResult:
  case Accepted(event: Event)
  case Skipped(row: SkippedRow)
  case Rejected(row: RejectedRow)

object CsvEventParser:
  private val dateFormatter = DateTimeFormatter.ofPattern("dd-MM-yyyy")
  private val timestampFormatter = new DateTimeFormatterBuilder()
    .appendPattern("dd-MM-yyyy HH:mm:ss")
    .optionalStart()
    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
    .optionalEnd()
    .toFormatter()
  private val outputTimestampFormatter = new DateTimeFormatterBuilder()
    .appendPattern("yyyy-MM-dd'T'HH:mm:ss")
    .appendFraction(ChronoField.NANO_OF_SECOND, 3, 9, true)
    .toFormatter()

  def parse(rowNumber: Long, row: Map[String, String]): ParseResult =
    required(row, "id", rowNumber) match
      case Left(rejected) => ParseResult.Rejected(rejected)
      case Right(symbol) =>
        required(row, "sectype", rowNumber) match
          case Left(rejected) => ParseResult.Rejected(rejected)
          case Right(securityType) =>
            row.get("last").map(_.trim).filter(_.nonEmpty) match
              case None => ParseResult.Skipped(SkippedRow(rowNumber, "non-price event: missing last"))
              case Some(priceText) =>
                val parsed = for
                  price <- parsePrice(priceText, rowNumber)
                  tradingTime <- required(row, "trading time", rowNumber)
                  date <- required(row, "trading date", rowNumber)
                  _ <- validateDate(date, rowNumber)
                  timestamp <- parseTimestamp(date, tradingTime, rowNumber)
                yield ParseResult.Accepted(Event(symbol, securityType, price, timestamp))
                parsed match
                  case Right(result) => result
                  case Left(rejected) => ParseResult.Rejected(rejected)

  def normalizeHeaders(row: Map[String, String]): Map[String, String] =
    row.map { case (key, value) => normalizeHeader(key) -> value }

  private def normalizeHeader(value: String): String =
    value.trim.toLowerCase

  private def required(row: Map[String, String], column: String, rowNumber: Long): Either[RejectedRow, String] =
    row.get(column).map(_.trim).filter(_.nonEmpty).toRight(
      RejectedRow(rowNumber, s"missing required field: $column")
    )

  private def parsePrice(value: String, rowNumber: Long): Either[RejectedRow, Double] =
    Try(value.toDouble).toOption.filter(price => price.isFinite && price > 0).toRight(
      RejectedRow(rowNumber, s"invalid price: $value")
    )

  private def validateDate(value: String, rowNumber: Long): Either[RejectedRow, Unit] =
    Try(java.time.LocalDate.parse(value, dateFormatter)).toOption.toRight(
      RejectedRow(rowNumber, s"invalid trading date: $value")
    ).map(_ => ())

  private def parseTimestamp(
      date: String,
      time: String,
      rowNumber: Long
  ): Either[RejectedRow, String] =
    Try(LocalDateTime.parse(s"$date $time", timestampFormatter))
      .toOption
      .toRight(RejectedRow(rowNumber, s"invalid timestamp: $date $time"))
      .map(_.format(outputTimestampFormatter))