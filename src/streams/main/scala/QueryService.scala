import com.sun.net.httpserver.{HttpExchange, HttpServer}
import java.io.IOException
import java.net.{InetSocketAddress, URI, URLDecoder, URLEncoder}
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets
import java.time.{Duration, Instant}
import java.util.concurrent.{Executors, TimeUnit}
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, KeyQueryMetadata, StoreQueryParameters}
import org.apache.kafka.streams.state.{HostInfo, QueryableStoreTypes}
import scala.util.control.NonFatal

/** Minimal HTTP front end for Kafka Streams interactive queries.
  *
  * The EMA time series is already materialised in the `ema-history` window
  * store, partitioned by symbol exactly like the input topic. Serving the UI
  * from that store avoids re-materialising the same state in every UI replica
  * and lets history outlive topic retention.
  *
  * Because the store is partitioned, an arbitrary instance may not hold the
  * symbol being asked for. `queryMetadataForKey` names the instance that does,
  * and this service proxies the request there, so any replica can answer any
  * query and the UI can simply talk to the `streams` service name.
  *
  * Built on the JDK's own HTTP server and client so the streams module gains no
  * new dependencies.
  */
final class QueryService(streams: KafkaStreams, advertisedHost: String, port: Int):

  private val self: HostInfo =
    advertisedHost.split(":", 2) match
      case Array(h, p) => new HostInfo(h, p.toInt)
      case _           => new HostInfo(advertisedHost, port)

  private val selfLabel: String = s"${self.host}:${self.port}"

  private val client: HttpClient =
    HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()

  private val server: HttpServer = HttpServer.create(new InetSocketAddress(port), 0)
  private val pool = Executors.newFixedThreadPool(8)

  server.setExecutor(pool)
  server.createContext("/health", handle(health))
  server.createContext("/ema", handle(ema))

  def start(): Unit = server.start()

  def stop(): Unit =
    server.stop(0)
    pool.shutdownNow()
    pool.awaitTermination(5, TimeUnit.SECONDS)
    ()

  // -- handlers -------------------------------------------------------------

  private def health(params: Map[String, String]): (Int, String) =
    val state = streams.state()
    val code = if state == KafkaStreams.State.RUNNING then 200 else 503
    (code, ujson.Obj("state" -> state.toString, "host" -> selfLabel).render())

  private def ema(params: Map[String, String]): (Int, String) =
    params.get("symbol").map(_.trim).filter(_.nonEmpty) match
      case None => (400, error("missing required parameter: symbol"))
      case Some(symbol) =>
        val from = params.get("from").flatMap(_.toLongOption).getOrElse(0L)
        val to = params.get("to").flatMap(_.toLongOption).getOrElse(Long.MaxValue)
        val limit = params.get("limit").flatMap(_.toIntOption).filter(_ > 0).getOrElse(2000)
        val localOnly = params.contains("local")

        val metadata =
          try streams.queryMetadataForKey(StreamsApp.HistoryStore, symbol, Serdes.String().serializer())
          catch case NonFatal(_) => KeyQueryMetadata.NOT_AVAILABLE

        if metadata == KeyQueryMetadata.NOT_AVAILABLE then
          (503, error("state store not available yet; streams is still rebalancing or restoring"))
        else if metadata.activeHost != self && !localOnly then
          proxy(metadata.activeHost, symbol, from, to, limit)
        else
          readLocal(symbol, from, to, limit)

  /** Reads the window store on this instance. The iterator is already ordered
    * by window start, so the newest `limit` points are simply the tail.
    */
  private def readLocal(symbol: String, from: Long, to: Long, limit: Int): (Int, String) =
    try
      val store = streams.store(
        StoreQueryParameters.fromNameAndType(
          StreamsApp.HistoryStore,
          QueryableStoreTypes.windowStore[String, String]()
        )
      )
      val iterator = store.fetch(symbol, Instant.ofEpochMilli(from), Instant.ofEpochMilli(clampUpper(to)))
      try
        val builder = Vector.newBuilder[String]
        while iterator.hasNext do builder += iterator.next().value
        val points = builder.result().takeRight(limit)
        // Stored values are already complete JSON objects; splice them in
        // rather than parsing and re-rendering every point.
        (200, s"""{"symbol":${ujson.Str(symbol).render()},"host":"$selfLabel","points":[${points.mkString(",")}]}""")
      finally iterator.close()
    catch
      case NonFatal(e) =>
        (503, error(s"store query failed: ${e.getClass.getSimpleName}: ${e.getMessage}"))

  private def proxy(host: HostInfo, symbol: String, from: Long, to: Long, limit: Int): (Int, String) =
    val uri = URI.create(
      s"http://${host.host}:${host.port}/ema" +
        s"?symbol=${enc(symbol)}&from=$from&to=$to&limit=$limit&local=1"
    )
    try
      val request = HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(10)).GET().build()
      val response = client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8))
      (response.statusCode, response.body)
    catch
      case NonFatal(e) =>
        (502, error(s"failed to reach the instance owning '$symbol' at $host: ${e.getMessage}"))

  // -- plumbing -------------------------------------------------------------

  /** `Instant.ofEpochMilli(Long.MaxValue)` overflows; cap at a far-future date. */
  private def clampUpper(to: Long): Long =
    if to > 253402300799000L then 253402300799000L else to

  private def error(message: String): String =
    ujson.Obj("error" -> message).render()

  private def enc(value: String): String =
    URLEncoder.encode(value, StandardCharsets.UTF_8)

  private def parseQuery(raw: String): Map[String, String] =
    if raw == null || raw.isEmpty then Map.empty
    else
      raw
        .split("&")
        .iterator
        .filter(_.nonEmpty)
        .map { pair =>
          pair.split("=", 2) match
            case Array(k, v) => URLDecoder.decode(k, StandardCharsets.UTF_8) -> URLDecoder.decode(v, StandardCharsets.UTF_8)
            case Array(k)    => URLDecoder.decode(k, StandardCharsets.UTF_8) -> ""
        }
        .toMap

  private def handle(fn: Map[String, String] => (Int, String)): com.sun.net.httpserver.HttpHandler =
    (exchange: HttpExchange) =>
      try
        val (status, body) =
          if exchange.getRequestMethod != "GET" then (405, error("only GET is supported"))
          else
            try fn(parseQuery(exchange.getRequestURI.getQuery))
            catch case NonFatal(e) => (500, error(s"${e.getClass.getSimpleName}: ${e.getMessage}"))

        val bytes = body.getBytes(StandardCharsets.UTF_8)
        exchange.getResponseHeaders.add("Content-Type", "application/json")
        // The UI is served from a different origin than this service.
        exchange.getResponseHeaders.add("Access-Control-Allow-Origin", "*")
        exchange.sendResponseHeaders(status, bytes.length.toLong)
        val out = exchange.getResponseBody
        try out.write(bytes)
        finally out.close()
      catch case _: IOException => ()
      finally exchange.close()
