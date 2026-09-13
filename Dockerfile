FROM eclipse-temurin:21-jre

WORKDIR /app
COPY target/scala-3.3.4/cs-e4780-ingestion-assembly-0.1.0-SNAPSHOT.jar /app/ingestion.jar

ENTRYPOINT ["java", "-Xms256m", "-Xmx1g", "-cp", "/app/ingestion.jar", "IngestionApp"]