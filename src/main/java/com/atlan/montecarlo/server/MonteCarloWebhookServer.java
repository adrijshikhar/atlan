/*
                                * Copyright (c) 2025 Atlan Inc.
                                */
package com.atlan.montecarlo.server;

import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

import com.atlan.montecarlo.grpc.*;
import com.atlan.montecarlo.service.KafkaProducerService;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MonteCarloWebhookServer {
  private final int port;
  private final Server server;
  private final KafkaProducerService kafkaProducer;

  public MonteCarloWebhookServer(int port, KafkaProducerService kafkaProducer) {
    this.port = port;
    this.kafkaProducer = kafkaProducer;
    this.server =
        ServerBuilder.forPort(port)
            .addService(new MonteCarloWebhookServiceImpl(kafkaProducer))
            .addService(ProtoReflectionService.newInstance())
            .build();
  }

  public void start() throws IOException {
    server.start();
    log.info("Server started, listening on port {}", port);
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  try {
                    MonteCarloWebhookServer.this.stop();
                  } catch (InterruptedException e) {
                    log.error("Error during server shutdown", e);
                  }
                }));
  }

  public void stop() throws InterruptedException {
    if (server != null) {
      server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
    }
  }

  public void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }

  @Slf4j
  static class MonteCarloWebhookServiceImpl
      extends MonteCarloWebhookServiceGrpc.MonteCarloWebhookServiceImplBase {
    private final KafkaProducerService kafkaProducer;

    MonteCarloWebhookServiceImpl(KafkaProducerService kafkaProducer) {
      this.kafkaProducer = kafkaProducer;
    }

    @Override
    public void processWebhook(
        WebhookRequest request, StreamObserver<WebhookResponse> responseObserver) {
      log.info("Received webhook request for event: {}", request.getEventId());

      try {
        // Validate request
        if (!validateRequest(request)) {
          sendErrorResponse(request, responseObserver, Status.VALIDATION_FAILED, "Invalid request");
          return;
        }

        // Send to Kafka
        kafkaProducer.sendEvent(request);

        // Send success response
        WebhookResponse response =
            WebhookResponse.newBuilder()
                .setEventId(request.getEventId())
                .setStatus(Status.SUCCESS)
                .setMessage("Event processed successfully")
                .setProcessedAt(
                    com.google.protobuf.Timestamp.newBuilder()
                        .setSeconds(Instant.now().getEpochSecond())
                        .setNanos(Instant.now().getNano())
                        .build())
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();

      } catch (Exception e) {
        log.error("Error processing webhook request", e);
        sendErrorResponse(request, responseObserver, Status.ERROR, "Internal server error");
      }
    }

    @Override
    public StreamObserver<WebhookRequest> streamEvents(
        StreamObserver<WebhookResponse> responseObserver) {
      return new StreamObserver<>() {
        @Override
        public void onNext(WebhookRequest request) {
          log.info("Received streaming event: {}", request.getEventId());
          try {
            if (validateRequest(request)) {
              kafkaProducer.sendEvent(request);
              sendSuccessResponse(request, responseObserver);
            } else {
              sendErrorResponse(
                  request, responseObserver, Status.VALIDATION_FAILED, "Invalid request");
            }
          } catch (Exception e) {
            log.error("Error processing streaming event", e);
            sendErrorResponse(request, responseObserver, Status.ERROR, "Internal server error");
          }
        }

        @Override
        public void onError(Throwable t) {
          log.error("Error in stream", t);
        }

        @Override
        public void onCompleted() {
          responseObserver.onCompleted();
        }
      };
    }

    private boolean validateRequest(WebhookRequest request) {
      return request != null
          && request.getEventId() != null
          && !request.getEventId().isEmpty()
          && request.getTableId() != null
          && !request.getTableId().isEmpty()
          && request.getTenantId() != null
          && !request.getTenantId().isEmpty()
          && request.getIssueType() != IssueType.UNKNOWN
          && request.getSeverity() != Severity.UNDEFINED;
    }

    private void sendSuccessResponse(
        WebhookRequest request, StreamObserver<WebhookResponse> responseObserver) {
      WebhookResponse response =
          WebhookResponse.newBuilder()
              .setEventId(request.getEventId())
              .setStatus(Status.SUCCESS)
              .setMessage("Event processed successfully")
              .setProcessedAt(
                  com.google.protobuf.Timestamp.newBuilder()
                      .setSeconds(Instant.now().getEpochSecond())
                      .setNanos(Instant.now().getNano())
                      .build())
              .build();
      responseObserver.onNext(response);
    }

    private void sendErrorResponse(
        WebhookRequest request,
        StreamObserver<WebhookResponse> responseObserver,
        Status status,
        String message) {
      WebhookResponse response =
          WebhookResponse.newBuilder()
              .setEventId(request.getEventId())
              .setStatus(status)
              .setMessage(message)
              .setProcessedAt(
                  com.google.protobuf.Timestamp.newBuilder()
                      .setSeconds(Instant.now().getEpochSecond())
                      .setNanos(Instant.now().getNano())
                      .build())
              .build();
      responseObserver.onNext(response);
      if (status != Status.SUCCESS) {
        responseObserver.onCompleted();
      }
    }
  }
}
