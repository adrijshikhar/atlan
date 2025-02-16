package com.atlan.montecarlo;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.atlan.montecarlo.config.ConfigurationLoader;
import com.atlan.montecarlo.config.ServerConfig;
import com.atlan.montecarlo.flink.MonteCarloEventProcessor;
import com.atlan.montecarlo.server.MonteCarloWebhookServer;
import com.atlan.montecarlo.service.KafkaProducerService;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Application {
  public static void main(String[] args) {
    try {
      // Load configuration
      String configPath = System.getProperty("config.path");
      ServerConfig config = ConfigurationLoader.loadConfiguration(configPath);
      log.info("Starting server with configuration: {}", config);

      // Initialize Kafka producer
      KafkaProducerService kafkaProducer =
          new KafkaProducerService.Builder()
              .bootstrapServers(config.getKafka().getBootstrapServers())
              .topic(config.getKafka().getMainTopic())
              .deadLetterTopic(config.getKafka().getDeadLetterTopic())
              .build();

      // Initialize and start the gRPC server
      MonteCarloWebhookServer server =
          new MonteCarloWebhookServer(config.getServer().getPort(), kafkaProducer);

      // Create an executor service for running the Flink processor
      ExecutorService executorService = Executors.newSingleThreadExecutor();

      // Initialize the Flink processor
      MonteCarloEventProcessor processor =
          new MonteCarloEventProcessor(
              config.getKafka().getBootstrapServers(),
              config.getKafka().getMainTopic(),
              config.getAtlas().getUrl(),
              config.getAtlas().getUsername(),
              config.getAtlas().getPassword());

      // Start the gRPC server
      server.start();
      log.info(
          "Monte Carlo webhook receiver started successfully on port {}",
          config.getServer().getPort());

      // Start the Flink processor in a separate thread
      Future<?> processorFuture =
          executorService.submit(
              () -> {
                try {
                  log.info("Starting Monte Carlo event processor...");
                  processor.startProcessing();
                } catch (Exception e) {
                  log.error("Error in event processor", e);
                }
              });

      // Add shutdown hook
      Runtime.getRuntime()
          .addShutdownHook(
              new Thread(
                  () -> {
                    try {
                      log.info("Shutting down...");
                      server.stop();
                      executorService.shutdown();
                      kafkaProducer.close();
                    } catch (Exception e) {
                      log.error("Error during shutdown", e);
                    }
                  }));

      // Keep the main thread alive
      server.blockUntilShutdown();
    } catch (Exception e) {
      log.error("Failed to start application", e);
      System.exit(1);
    }
  }
}
