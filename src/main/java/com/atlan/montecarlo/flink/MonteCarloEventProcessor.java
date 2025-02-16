/*
 * Copyright (c) 2025 Atlan Inc.
 */
package com.atlan.montecarlo.flink;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import com.atlan.montecarlo.service.AtlasService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MonteCarloEventProcessor implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String bootstrapServers;
  private final String sourceTopic;
  private final String atlasUrl;
  private final String atlasUsername;
  private final String atlasPassword;

  public MonteCarloEventProcessor(
      String bootstrapServers,
      String sourceTopic,
      String atlasUrl,
      String atlasUsername,
      String atlasPassword) {
    this.bootstrapServers = bootstrapServers;
    this.sourceTopic = sourceTopic;
    this.atlasUrl = atlasUrl;
    this.atlasUsername = atlasUsername;
    this.atlasPassword = atlasPassword;

    log.info("Starting Monte Carlo event processor...");
    log.info("Atlas URL: {}", atlasUrl);
    log.info("Atlas Username: {}", atlasUsername);
    log.info("Atlas Password: {}", atlasPassword);
  }

  public void startProcessing() throws Exception {
    // Create the execution environment with the necessary configuration
    Configuration flinkConfig = new Configuration();
    flinkConfig.setString("taskmanager.memory.process.size", "1024m");
    // set worker to one
    flinkConfig.setInteger("parallelism.default", 1);
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment(flinkConfig);

    // Configure Kafka source
    KafkaSource<String> source =
        KafkaSource.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics(sourceTopic)
            .setGroupId("monte-carlo-processor")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

    DataStream<String> stream =
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

    // Process events
    stream
        .map(new MonteCarloEventMapper())
        .name("Event Processor")
        .addSink(new MonteCarloAtlasSink(atlasUrl, atlasUsername, atlasPassword))
        .name("Atlas Update Sink");

    env.execute("Monte Carlo Event Processor");
  }

  @Data
  public static class AtlasUpdateEvent implements Serializable {
    private static final long serialVersionUID = 1L;
    private String tableId;
    private String issueType;
    private String severity;
    private Map<String, Object> metadata;
  }

  public static class MonteCarloEventMapper extends RichMapFunction<String, AtlasUpdateEvent> {
    private transient ObjectMapper objectMapper;

    @Override
    public void open(Configuration parameters) {
      objectMapper = JsonMapper.builder().findAndAddModules().build();
    }

    @Override
    public AtlasUpdateEvent map(String value) throws Exception {
      try {
        JsonNode jsonNode = objectMapper.readTree(value);

        AtlasUpdateEvent updateEvent = new AtlasUpdateEvent();
        updateEvent.setTableId(jsonNode.get("tableId").asText());
        updateEvent.setIssueType(jsonNode.get("issueType").asText());
        updateEvent.setSeverity(jsonNode.get("severity").asText());

        Map<String, Object> metadata = new HashMap<>();
        JsonNode metadataNode = jsonNode.get("metadata");
        if (metadataNode != null && metadataNode.isObject()) {
          metadataNode
              .fields()
              .forEachRemaining(entry -> metadata.put(entry.getKey(), entry.getValue().asText()));
        }
        updateEvent.setMetadata(metadata);

        return updateEvent;
      } catch (Exception e) {
        log.error("Error processing event: {}", value, e);
        throw e;
      }
    }
  }

  public static class MonteCarloAtlasSink extends RichSinkFunction<AtlasUpdateEvent> {
    private static final long serialVersionUID = 1L;
    private final String atlasUrl;
    private final String atlasUsername;
    private final String atlasPassword;
    private transient AtlasService atlasService;

    public MonteCarloAtlasSink(String atlasUrl, String atlasUsername, String atlasPassword) {
      this.atlasUrl = atlasUrl;
      this.atlasUsername = atlasUsername;
      this.atlasPassword = atlasPassword;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      atlasService = new AtlasService(atlasUrl, atlasUsername, atlasPassword);
      try {
        atlasService.createMonteCarloClassificationType();
      } catch (Exception e) {
        log.warn("Classification type might already exist: {}", e.getMessage());
      }
    }

    @Override
    public void invoke(AtlasUpdateEvent event, Context context) throws Exception {
      try {
        atlasService.updateTableMetadata(
            event.getTableId(), event.getIssueType(), event.getSeverity(), event.getMetadata());
        log.info("Successfully updated Atlas metadata for table: {}", event.getTableId());
      } catch (Exception e) {
        log.error("Error updating Atlas metadata: {}", e.getMessage(), e);
        throw e;
      }
    }
  }
}
