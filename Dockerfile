# Build the deployable fat jar, then run it on a JRE.
#   docker build -t strata-server .
#   docker run -e STRATA_ROLE=meta -e STRATA_ZK_CONNECT=zk:2181 strata-server
# (or just `docker compose up` — see docker-compose.yml)

FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
# Copy poms first so dependency resolution layers cache independently of source changes.
COPY pom.xml ./
COPY strata-common/pom.xml strata-common/
COPY strata-proto/pom.xml strata-proto/
COPY strata-format/pom.xml strata-format/
COPY strata-node/pom.xml strata-node/
COPY strata-meta/pom.xml strata-meta/
COPY strata-client/pom.xml strata-client/
COPY strata-server/pom.xml strata-server/
COPY strata-it/pom.xml strata-it/
COPY strata-coverage/pom.xml strata-coverage/
RUN mvn -q -pl strata-server -am -DskipTests dependency:go-offline || true
# Now the sources.
COPY . .
RUN mvn -q -pl strata-server -am -DskipTests package

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /src/strata-server/target/strata-server.jar /app/strata-server.jar
# Storage nodes persist chunk data here; mount a volume in production.
VOLUME ["/data"]
# Default JVM flags; override with JAVA_OPTS. Container-aware heap sizing.
ENV JAVA_OPTS="-XX:MaxRAMPercentage=70 -XX:+ExitOnOutOfMemoryError"
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/strata-server.jar \"$@\"", "--"]
# Role: override with `command: [meta]` / `[node]` or STRATA_ROLE.
CMD ["node"]
