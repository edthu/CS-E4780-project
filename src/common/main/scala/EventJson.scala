/** JSON codec shared by the ingestion, producer, consumer and streams modules.
  *
  * `render` mirrors the object built in `IngestionApp.ingest`, so producer
  * output stays byte-compatible with the ingestion NDJSON handoff.
  */
object EventJson:
  def render(event: Event): String =
    ujson.Obj(
      "symbol" -> event.symbol,
      "securityType" -> event.securityType,
      "price" -> event.price,
      "timestamp" -> event.timestamp
    ).render()

  def parse(line: String): Event =
    val obj = ujson.read(line)
    Event(
      symbol = obj("symbol").str,
      securityType = obj("securityType").str,
      price = obj("price").num,
      timestamp = obj("timestamp").str
    )

  /** Cheap key extraction so the producer can route by symbol without fully
    * deserialising each line into an [[Event]].
    */
  def symbolOf(line: String): String =
    ujson.read(line)("symbol").str
