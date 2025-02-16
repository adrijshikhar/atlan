package com.atlan.montecarlo.service;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import com.atlan.montecarlo.grpc.WebhookRequest;
import com.google.protobuf.util.JsonFormat;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaProducerService implements AutoCloseable {
  private final KafkaProducer<String, String> producer;
  private final String topic;
  private final String deadLetterTopic;
  private final JsonFormat.Printer jsonPrinter;

  public KafkaProducerService(String bootstrapServers, String topic, String deadLetterTopic) {
    log.info("Initializing KafkaProducerService with bootstrap servers: {}", bootstrapServers);
    this.topic = topic;
    this.deadLetterTopic = deadLetterTopic;
    this.producer = createProducer(bootstrapServers);
    this.jsonPrinter =
        JsonFormat.printer().includingDefaultValueFields().omittingInsignificantWhitespace();
  }

  private KafkaProducer<String, String> createProducer(String bootstrapServers) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    log.debug("Creating Kafka producer with properties: {}", props);

    // Performance and reliability configurations
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.RETRIES_CONFIG, 3);
    props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
    props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
    props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 60000);
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

    return new KafkaProducer<>(props);
  }

  public Future<RecordMetadata> sendEvent(WebhookRequest request) {
    CompletableFuture<RecordMetadata> future = new CompletableFuture<>();

    try {
      // Convert protobuf message to JSON string
      String jsonValue = jsonPrinter.print(request);
      String key = request.getTenantId() + "-" + request.getEventId();

      ProducerRecord<String, String> record =
          new ProducerRecord<>(
              topic,
              // Use tenant ID as partition key for same-tenant ordering
              null, // Partition
              System.currentTimeMillis(), // Timestamp
              key,
              jsonValue);

      // Add custom headers
      record
          .headers()
          .add("event_type", request.getEventType().name().getBytes())
          .add("issue_type", request.getIssueType().name().getBytes())
          .add("severity", request.getSeverity().name().getBytes())
          .add("tenant_id", request.getTenantId().getBytes());

      producer.send(
          record,
          (metadata, exception) -> {
            if (exception != null) {
              log.error("Failed to send event to Kafka", exception);
              handleFailedEvent(request, exception);
              future.completeExceptionally(exception);
            } else {
              log.info(
                  "Event sent successfully to topic {}, partition {}, offset {}",
                  metadata.topic(),
                  metadata.partition(),
                  metadata.offset());
              future.complete(metadata);
            }
          });

    } catch (Exception e) {
      log.error("Error preparing event for Kafka", e);
      handleFailedEvent(request, e);
      future.completeExceptionally(e);
    }

    return future;
  }

  private void handleFailedEvent(WebhookRequest request, Exception exception) {
    try {
      // Convert to JSON for dead letter queue
      String jsonValue = jsonPrinter.print(request);
      String key = request.getTenantId() + "-" + request.getEventId();

      ProducerRecord<String, String> deadLetterRecord =
          new ProducerRecord<>(deadLetterTopic, key, jsonValue);

      // Add error information in headers
      deadLetterRecord
          .headers()
          .add("error_message", exception.getMessage().getBytes())
          .add("error_type", exception.getClass().getName().getBytes())
          .add("failed_timestamp", String.valueOf(System.currentTimeMillis()).getBytes());

      producer.send(
          deadLetterRecord,
          (metadata, ex) -> {
            if (ex != null) {
              log.error("Failed to send event to dead letter queue", ex);
            } else {
              log.info(
                  "Event sent to dead letter queue: topic={}, partition={}, offset={}",
                  metadata.topic(),
                  metadata.partition(),
                  metadata.offset());
            }
          });
    } catch (Exception e) {
      log.error("Failed to handle failed event", e);
    }
  }

  public void flush() {
    producer.flush();
  }

  @Override
  public void close() {
    if (producer != null) {
      try {
        producer.flush();
        producer.close();
      } catch (Exception e) {
        log.error("Error closing Kafka producer", e);
      }
    }
  }

  // Builder pattern for creating KafkaProducerService
  public static class Builder {
    private String bootstrapServers;
    private String topic;
    private String deadLetterTopic;

    public Builder bootstrapServers(String bootstrapServers) {
      this.bootstrapServers = bootstrapServers;
      return this;
    }

    public Builder topic(String topic) {
      this.topic = topic;
      return this;
    }

    public Builder deadLetterTopic(String deadLetterTopic) {
      this.deadLetterTopic = deadLetterTopic;
      return this;
    }

    public KafkaProducerService build() {
      if (bootstrapServers == null || topic == null || deadLetterTopic == null) {
        throw new IllegalStateException("All configuration parameters must be set");
      }
      return new KafkaProducerService(bootstrapServers, topic, deadLetterTopic);
    }
  }
}
