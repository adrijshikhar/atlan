package com.atlan.montecarlo.flink;

import com.atlan.montecarlo.service.AtlasService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.Collections;
import java.util.List;

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
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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
      objectMapper = new ObjectMapper();
    }

    @Override
    public MetadataUpdateEvent map(String value) throws Exception {
      try {
        JsonNode jsonNode = objectMapper.readTree(value);
        String eventType = jsonNode.get("eventType").asText();

        MetadataUpdateEvent updateEvent = new MetadataUpdateEvent();
        updateEvent.setEventType(eventType);

        switch (eventType) {
          case "MONTE_CARLO_ALERT":
            // Handle Monte Carlo alert event
            updateEvent.setTableId(jsonNode.get("table_id").asText());
            updateEvent.setIssueType(jsonNode.get("issue_type").asText());
            updateEvent.setSeverity(jsonNode.get("severity").asText());
            updateEvent.setMetadata(jsonNode.get("metadata").toString());
            break;

          case "PII_CLASSIFICATION":
            // Handle PII classification event
            updateEvent.setTableId(jsonNode.get("table_id").asText());
            updateEvent.setPiiLevel(jsonNode.get("pii_level").asText());
            updateEvent.setPiiElements(
                objectMapper.convertValue(
                    jsonNode.get("pii_elements"),
                    objectMapper
                        .getTypeFactory()
                        .constructCollectionType(List.class, String.class)));
            break;

          default:
            log.warn("Unknown event type: {}", eventType);
            return null;
        }

        return updateEvent;
      } catch (Exception e) {
        log.error("Error processing event: {}", value, e);
        throw e;
      }
    }
  }

  @Slf4j
  private static class MonteCarloAtlasSink extends RichSinkFunction<MetadataUpdateEvent> {
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
          case "MONTE_CARLO_ALERT":
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
            break;

          case "PII_CLASSIFICATION":
            // Apply PII classification
            atlasService.applyPIIClassification(
                event.getTableId(), event.getPiiElements(), event.getPiiLevel());
            log.info(
                "Applied PII classification to table: {} with level: {}",
                event.getTableId(),
                event.getPiiLevel());
            break;
        }
      } catch (Exception e) {
        log.error("Error updating Atlas: {}", e.getMessage(), e);
      }
    }
  }

  @Data
  @NoArgsConstructor
  public static class MetadataUpdateEvent {
    private String eventType;
    private String tableId;

    // Monte Carlo alert fields
    private String issueType;
    private String severity;
    private String metadata;

    // PII classification fields
    private String piiLevel;
    private List<String> piiElements;
  }
}
