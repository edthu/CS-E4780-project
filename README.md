# CS-E4780 project
# Kafka demo
See mermaid diagram in [docs/ARCHITECTURE](docs/ARCHITECTURE.md)
## Running the pipeline

### With Docker Compose (recommended)

Everything runs as containers on one Compose network. App images copy in the fat
jars, so build the jars first.

```bash
# 1. Build the app jars (the Docker images COPY these in)
sbt ingestion/assembly producer/assembly consumer/assembly streams/assembly

# 2. Start the broker, the stream processor and the consumer
docker compose up -d --build kafka streams consumer

# 3. Generate NDJSON into ./output (needs the DEBS CSV in ./data — see
#    scripts/download-data.sh). Skip if ./output/events.ndjson already exists.
docker compose run --rm ingestion

# 4. Publish the NDJSON to the trading-events topic
docker compose run --rm producer

# 5. Watch the processed output
docker compose logs -f consumer

# Reset events with
./scripts/reset-topics.sh
```

Topics are auto-created on first use. Inside the Compose network the apps reach
the broker at `kafka:19092`; from the host it's `localhost:9092`.


