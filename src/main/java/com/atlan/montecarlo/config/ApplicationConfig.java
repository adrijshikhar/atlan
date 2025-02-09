/*
                                * Copyright (c) 2025 Atlan Inc.
                                */
package com.atlan.montecarlo.config;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class ApplicationConfig {
  private final KafkaConfig kafkaConfig;
  private final ServerConfig serverConfig;

  @Getter
  @Builder
  public static class KafkaConfig {
    private final String bootstrapServers;
    private final String mainTopic;
    private final String deadLetterTopic;

    public static KafkaConfig defaultConfig() {
      return KafkaConfig.builder()
          .bootstrapServers("localhost:9092")
          .mainTopic("monte-carlo-raw-events")
          .deadLetterTopic("monte-carlo-dead-letter")
          .build();
    }
  }

  @Getter
  @Builder
  public static class ServerConfig {
    private final int port;
    private final boolean useTls;
    private final String certPath;
    private final String keyPath;

    public static ServerConfig defaultConfig() {
      return ServerConfig.builder().port(9090).useTls(false).build();
    }
  }

  public static ApplicationConfig defaultConfig() {
    return ApplicationConfig.builder()
        .kafkaConfig(KafkaConfig.defaultConfig())
        .serverConfig(ServerConfig.defaultConfig())
        .build();
  }
}
