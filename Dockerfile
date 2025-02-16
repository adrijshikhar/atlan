FROM eclipse-temurin:17-jre-jammy


# Install wget for downloading grpc-health-probe
RUN apt-get update && apt-get install -y wget && \
    wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v0.4.19/grpc_health_probe-linux-amd64 && \
    chmod +x /bin/grpc_health_probe

WORKDIR /app

COPY target/webhook-receiver-1.0-SNAPSHOT.jar app.jar
COPY docker/config/application.yml application.yml

EXPOSE 8030

ENTRYPOINT ["java", \
    "--add-opens=java.base/java.lang=ALL-UNNAMED", \
    "--add-opens=java.base/java.nio=ALL-UNNAMED", \
    "--add-opens=java.base/java.util=ALL-UNNAMED", \
    "-Dconfig.path=/app/application.yml", \
    "-jar", "app.jar"]