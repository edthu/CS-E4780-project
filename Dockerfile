FROM eclipse-temurin:21-jre

# One image definition for every app module. MODULE selects the assembly jar
# (built by `sbt <module>/assembly`); MAIN_CLASS is the entrypoint class.
ARG MODULE
ARG MAIN_CLASS

WORKDIR /app
COPY src/${MODULE}/target/scala-3.3.4/${MODULE}.jar /app/app.jar
ENV MAIN_CLASS=${MAIN_CLASS}

# `"$@"` forwards the compose `command:` args (e.g. ingestion's csv/output paths).
ENTRYPOINT ["sh", "-c", "exec java -Xms256m -Xmx1g -cp /app/app.jar $MAIN_CLASS \"$@\"", "--"]
