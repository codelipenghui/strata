# Runtime image for the Strata server. The container only runs `java -jar` on a JRE, so this
# image just carries the pre-built fat jar — build it FIRST, then build the image:
#
#   mvn -q -pl strata-server -am -DskipTests package      # produces strata-server/target/strata-server.jar
#   docker build -t strata-server:local .
#
# or simply: ./scripts/build-image.sh   (does both)
#
# The first argument selects the role (data-node|controller|combined); configuration is via env vars —
# see StrataServer and docker-compose.yml.
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY strata-server/target/strata-server.jar /app/strata-server.jar
# Data nodes persist chunk data here; mount a volume in production.
VOLUME ["/data"]
# Document the listeners (so they show in `docker ps`). EXPOSE does NOT gate connectivity —
# same-network containers already reach these regardless; nothing is published to the host here:
#   9100 SCP (data + metadata, routed by opcode) · 9300 Prometheus /metrics
EXPOSE 9100 9300
# Default JVM flags; override with JAVA_OPTS. Container-aware heap sizing.
ENV JAVA_OPTS="-XX:MaxRAMPercentage=70 -XX:+ExitOnOutOfMemoryError"
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/strata-server.jar \"$@\"", "--"]
# Role: override with `command: [controller]` / `[data-node]` / `[combined]` or STRATA_ROLE.
CMD ["data-node"]
