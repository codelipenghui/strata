# Runtime image for the Strata server. The container only runs `java -jar` on a JRE, so this
# image just carries the pre-built fat jar — build it FIRST, then build the image:
#
#   mvn -q -pl strata-server -am -DskipTests package      # produces strata-server/target/strata-server.jar
#   docker build -t strata-server:local .
#
# or simply: ./scripts/build-image.sh   (does both)
#
# The first argument selects the role (node|meta); configuration is via env vars — see
# StrataServer and docker-compose.yml.
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY strata-server/target/strata-server.jar /app/strata-server.jar
# Storage nodes persist chunk data here; mount a volume in production.
VOLUME ["/data"]
# Default JVM flags; override with JAVA_OPTS. Container-aware heap sizing.
ENV JAVA_OPTS="-XX:MaxRAMPercentage=70 -XX:+ExitOnOutOfMemoryError"
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/strata-server.jar \"$@\"", "--"]
# Role: override with `command: [meta]` / `[node]` or STRATA_ROLE.
CMD ["node"]
