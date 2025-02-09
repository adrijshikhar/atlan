/*
 * Copyright (c) 2025 Atlan Inc.
 */
package com.atlan.montecarlo;

import com.atlan.montecarlo.config.ApplicationConfig;
import com.atlan.montecarlo.server.MonteCarloWebhookServer;
import com.atlan.montecarlo.service.KafkaProducerService;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Application {
  public static void main(String[] args) {
    try {
      // Load configuration
      ApplicationConfig config = ApplicationConfig.defaultConfig();

      // Initialize Kafka producer
      KafkaProducerService kafkaProducer =
          new KafkaProducerService.Builder()
              .bootstrapServers(config.getKafkaConfig().getBootstrapServers())
              .topic(config.getKafkaConfig().getMainTopic())
              .deadLetterTopic(config.getKafkaConfig().getDeadLetterTopic())
              .build();

      // Initialize and start the gRPC server
      MonteCarloWebhookServer server =
          new MonteCarloWebhookServer(config.getServerConfig().getPort(), kafkaProducer);

      // Start the server
      server.start();
      log.info("Monte Carlo webhook receiver started successfully");

      // Keep the application running
      server.blockUntilShutdown();
    } catch (Exception e) {
      log.error("Failed to start application", e);
      System.exit(1);
    }
  }
}
