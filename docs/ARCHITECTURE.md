# Architecture Diagrams

Mermaid diagrams of the Kafka pipeline for the CS-E4780 trading-tick project.
See [`KAFKA_PLAN.md`](KAFKA_PLAN.md) for the prose plan and the
plan.png → assignment commentary.

## Data flow (runtime)

How events move from the raw CSV through Kafka to the consumer/UI.

```mermaid
flowchart LR
    csv[("DEBS 2022 CSV<br/>(one file per day)")]
    ndjson[("events.ndjson<br/>(NDJSON, ./output)")]

    subgraph apps["Scala 3 apps (JVM)"]
        ingestion["ingestion<br/>IngestionApp"]
        producer["producer<br/>ProducerApp"]
        streams["streams<br/>StreamsApp<br/>mapValues(addMarker)"]
        consumer["consumer<br/>ConsumerApp<br/>ALL / SUMMARY logs"]
    end

    subgraph broker["Kafka broker (KRaft, single node)"]
        traw[["trading-events<br/>(keyed by symbol)"]]
        tproc[["trading-events-processed"]]
        tadv[["advisories<br/>(future: buy/sell)"]]
    end

    ui["UI / dashboard<br/>(bonus: Smart Visualization)"]

    csv --> ingestion --> ndjson --> producer
    producer -->|"key=symbol<br/>value=raw NDJSON line"| traw
    traw --> streams
    streams -->|"adds processedBy field"| tproc
    tproc --> consumer
    consumer --> ui

    streams -.->|"future: Query 2 crossovers"| tadv
    tadv -.-> ui

    classDef store fill:#f4f4f4,stroke:#999;
    classDef topic fill:#e8f0fe,stroke:#4285f4;
    class csv,ndjson store;
    class traw,tproc,tadv topic;
```

## Deployment (Docker Compose)

Each app is a fat jar baked into an image via one shared `Dockerfile`
(`ARG MODULE` / `ARG MAIN_CLASS`). All services share one Compose network and
reach the broker at `kafka:19092` (internal); the host uses `localhost:9092`.

```mermaid
flowchart TB
    subgraph net["Compose network"]
        kafka["kafka<br/>apache/kafka:3.8.1<br/>INTERNAL :19092 / EXTERNAL :9092"]

        ingestionS["ingestion<br/>(batch, run --rm)"]
        producerS["producer<br/>(batch, run --rm)"]
        streamsS["streams<br/>(long-running)"]
        consumerS["consumer<br/>(long-running)"]
    end

    outvol[("./output volume")]

    ingestionS -->|writes| outvol
    outvol -->|reads| producerS

    producerS -->|"KAFKA_BOOTSTRAP=kafka:19092"| kafka
    streamsS -->|depends_on: healthy| kafka
    consumerS -->|depends_on: healthy| kafka

    host["host: localhost:9092<br/>(local sbt runs)"] -.->|EXTERNAL listener| kafka

    classDef svc fill:#e8f0fe,stroke:#4285f4;
    class kafka,ingestionS,producerS,streamsS,consumerS svc;
```

## Build flow (source → running container)

Why `docker compose up --build` alone is not enough after a code change: the
image only **copies** a pre-built jar; it never compiles. Re-run `sbt assembly`
first so the jar is fresh, then rebuild the image.

```mermaid
flowchart LR
    src["src/&lt;module&gt;/main/scala/*.scala"]
    jar["src/&lt;module&gt;/target/scala-3.3.4/&lt;module&gt;.jar<br/>(fat jar)"]
    img["Docker image<br/>(COPY jar → /app/app.jar)"]
    ctr["running container"]

    src -->|"sbt &lt;module&gt;/assembly"| jar
    jar -->|"docker compose build (COPY)"| img
    img -->|"docker compose up -d"| ctr

    src -.->|"edit code only<br/>(jar stays stale!)"| jar
```

## Module dependencies (sbt build)

```mermaid
flowchart TB
    common["common<br/>Event, EventJson<br/>(ujson)"]
    ingestion["ingestion<br/>(commons-csv, ujson)"]
    producer["producer<br/>(kafka-clients)"]
    consumer["consumer<br/>(kafka-clients)"]
    streams["streams<br/>(kafka-streams)"]
    root["root (aggregate)"]

    ingestion --> common
    producer --> common
    consumer --> common
    streams --> common

    root -.aggregates.-> common
    root -.aggregates.-> ingestion
    root -.aggregates.-> producer
    root -.aggregates.-> consumer
    root -.aggregates.-> streams
```
