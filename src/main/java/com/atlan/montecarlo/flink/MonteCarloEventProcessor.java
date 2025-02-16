package com.atlan.montecarlo.flink;

import static com.atlan.montecarlo.flink.MonteCarloEventProcessor.MetadataUpdateEvent.EventType.MONTE_CARLO_ALERT;
import static com.atlan.montecarlo.flink.MonteCarloEventProcessor.MetadataUpdateEvent.EventType.PII_CLASSIFICATION;

import java.util.Collections;
import java.util.List;
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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MonteCarloEventProcessor {
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
            .setGroupId("metadata-processor")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

    DataStream<String> stream =
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

    stream
        .map(new EventProcessor())
        .name("Event Processor")
        .addSink(new MonteCarloAtlasSink(atlasUrl, atlasUsername, atlasPassword))
        .name("Atlas Update Sink");

    env.execute("Metadata Event Processor");
  }

  @Slf4j
  private static class EventProcessor extends RichMapFunction<String, MetadataUpdateEvent> {
    private transient ObjectMapper objectMapper;

    @Override
    public void open(Configuration parameters) {
      objectMapper =
          new ObjectMapper().setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
    }

    @Override
    public MetadataUpdateEvent map(String value) throws Exception {
      try {
        JsonNode node = objectMapper.readTree(value);
        log.info("Processing event: {}", node);

        MetadataUpdateEvent event = new MetadataUpdateEvent();
        event.setEventType(MetadataUpdateEvent.EventType.valueOf(node.get("eventType").asText()));
        event.setTableId(node.get("tableId").asText());

        if (MONTE_CARLO_ALERT.equals(event.getEventType())) {
          event.setIssueType(node.get("issueType").asText());
          event.setSeverity(node.get("severity").asText());
          if (node.has("metadata")) {
            event.setMetadata(
                objectMapper.convertValue(
                    node.get("metadata"), new TypeReference<Map<String, Object>>() {}));
          }
        } else if (PII_CLASSIFICATION.equals(event.getEventType())) {
          event.setPiiLevel(node.get("piiLevel").asText());
          if (node.has("piiElements")) {
            event.setPiiElements(
                objectMapper.convertValue(
                    node.get("piiElements"), new TypeReference<List<String>>() {}));
          }
        }

        return event;
      } catch (Exception e) {
        log.error("Error processing event: {}", value, e);
        throw e;
      }
    }
  }

  @Slf4j
  private static class MonteCarloAtlasSink extends RichSinkFunction<MetadataUpdateEvent> {
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
    public void open(Configuration parameters) {
      try {
        atlasService = new AtlasService(atlasUrl, atlasUsername, atlasPassword);
      } catch (Exception e) {
        log.error("Error initializing Atlas service: {}", e.getMessage(), e);
      }
    }

    @Override
    public void invoke(MetadataUpdateEvent event, Context context) {
      try {
        switch (event.getEventType()) {
          case MONTE_CARLO_ALERT -> {
            // Create alert entity
            atlasService.createAlertEntity(
                event.getTableId(),
                event.getIssueType(),
                event.getSeverity(),
                Collections.singletonMap("details", event.getMetadata()));
            log.info(
                "Created Monte Carlo alert for table: {} with issue: {}",
                event.getTableId(),
                event.getIssueType());
          }
          case PII_CLASSIFICATION -> {
            // Apply PII classification
            atlasService.applyPIIClassification(
                event.getTableId(), event.getPiiElements(), event.getPiiLevel());
            log.info(
                "Applied PII classification to table: {} with level: {}",
                event.getTableId(),
                event.getPiiLevel());
          }
        }
      } catch (Exception e) {
        log.error("Error updating Atlas: {}", e.getMessage(), e);
      }
    }
  }

  @Data
  @NoArgsConstructor
  @JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
  public static class MetadataUpdateEvent {
    private EventType eventType;
    private String tableId;

    // Monte Carlo alert fields
    private String issueType;
    private String severity;
    private Map<String, Object> metadata;

    // PII classification fields
    private String piiLevel;
    private List<String> piiElements;

    enum EventType {
      MONTE_CARLO_ALERT,
      PII_CLASSIFICATION
    }
  }
}
