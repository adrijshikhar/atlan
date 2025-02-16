package com.atlan.montecarlo.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ServerConfig {
  private Server server = new Server();
  private Kafka kafka = new Kafka();
  private Atlas atlas = new Atlas();
  private Monitoring monitoring = new Monitoring();

  @Data
  @NoArgsConstructor
  public static class Server {
    private int port = 8030;
    private boolean useTls = false;
  }

  @Data
  @NoArgsConstructor
  public static class Kafka {
    private String bootstrapServers = "localhost:9092";
    private String mainTopic = "monte-carlo-raw-events";
    private String deadLetterTopic = "monte-carlo-dead-letter";
  }

  @Data
  @NoArgsConstructor
  public static class Atlas {
    private String url = "http://localhost:21000";
    private String username = "admin";
    private String password = "admin";
  }

  @Data
  @NoArgsConstructor
  public static class Monitoring {
    private boolean enabled = true;
    private int metricsPort = 8080;
  }

  public static ServerConfig defaultConfig() {
    return new ServerConfig();
  }
}
