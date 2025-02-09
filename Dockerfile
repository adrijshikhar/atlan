FROM eclipse-temurin:17-jre-jammy

WORKDIR /app

# Create config directory
RUN mkdir -p /app/config

# Copy the pre-built JAR file and config
COPY target/webhook-receiver-1.0-SNAPSHOT-jar-with-dependencies.jar app.jar
COPY docker/config/application.yml /app/config/application.yml

EXPOSE 8030
EXPOSE 8080

# Run the application with the default config path
ENTRYPOINT ["java", "-Dconfig.path=/app/config/application.yml", "-jar", "app.jar"]