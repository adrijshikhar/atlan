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

  public MonteCarloWebhookServer(int port, KafkaProducerService kafkaProducer) {
    this.port = port;
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
      String requestId = request.getEventId();
      log.info("Received webhook request: {}", requestId);

      try {
        ValidationResult validation = validateRequest(request);
        if (!validation.isValid()) {
          sendErrorResponse(
              request,
              responseObserver,
              Status.VALIDATION_FAILED,
              String.format("Invalid request: %s", validation.getMessage()));
          return;
        }

        kafkaProducer.sendEvent(request);
        sendSuccessResponse(request, responseObserver);

        log.info("Successfully processed webhook request: {}", requestId);
      } catch (Exception e) {
        log.error("Error processing webhook request: {}", requestId, e);
        sendErrorResponse(
            request, responseObserver, Status.ERROR, "Internal server error: " + e.getMessage());
      } finally {
        // Complete the response stream
        responseObserver.onCompleted();
      }
    }

    @Override
    public StreamObserver<WebhookRequest> streamEvents(
        StreamObserver<WebhookResponse> responseObserver) {
      return new StreamObserver<>() {
        @Override
        public void onNext(WebhookRequest request) {
          String requestId = request.getEventId();
          log.info("Received streaming event: {}", requestId);

          try {
            ValidationResult validation = validateRequest(request);
            if (!validation.isValid()) {
              sendErrorResponse(
                  request,
                  responseObserver,
                  Status.VALIDATION_FAILED,
                  String.format("Invalid request: %s", validation.getMessage()));
              return;
            }

            kafkaProducer.sendEvent(request);
            sendSuccessResponse(request, responseObserver);

            log.info("Successfully processed streaming event: {}", requestId);
          } catch (Exception e) {
            log.error("Error processing streaming event: {}", requestId, e);
            sendErrorResponse(
                request,
                responseObserver,
                Status.ERROR,
                "Internal server error: " + e.getMessage());
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

    private ValidationResult validateRequest(WebhookRequest request) {
      if (request == null) {
        return new ValidationResult(false, "Request cannot be null");
      }

      if (isNullOrEmpty(request.getEventId())) {
        return new ValidationResult(false, "Event ID is required");
      }

      if (isNullOrEmpty(request.getTableId())) {
        return new ValidationResult(false, "Table ID is required");
      }

      if (isNullOrEmpty(request.getTenantId())) {
        return new ValidationResult(false, "Tenant ID is required");
      }

      if (request.getIssueType() == IssueType.UNRECOGNIZED) {
        return new ValidationResult(false, "Valid issue type is required");
      }

      if (request.getSeverity() == Severity.UNRECOGNIZED) {
        return new ValidationResult(false, "Valid severity level is required");
      }

      return new ValidationResult(true, null);
    }

    private boolean isNullOrEmpty(String str) {
      return str == null || str.trim().isEmpty();
    }

    private void sendSuccessResponse(
        WebhookRequest request, StreamObserver<WebhookResponse> responseObserver) {
      WebhookResponse response =
          WebhookResponse.newBuilder()
              .setEventId(request.getEventId())
              .setStatus(Status.SUCCESS)
              .setMessage("Event processed successfully")
              .setProcessedAt(getCurrentTimestamp())
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
              .setProcessedAt(getCurrentTimestamp())
              .build();

      responseObserver.onNext(response);
      if (status != Status.SUCCESS) {
        responseObserver.onCompleted();
      }
    }

    private com.google.protobuf.Timestamp getCurrentTimestamp() {
      Instant now = Instant.now();
      return com.google.protobuf.Timestamp.newBuilder()
          .setSeconds(now.getEpochSecond())
          .setNanos(now.getNano())
          .build();
    }
  }

  private static class ValidationResult {
    private final boolean valid;
    private final String message;

    ValidationResult(boolean valid, String message) {
      this.valid = valid;
      this.message = message;
    }

    public boolean isValid() {
      return valid;
    }

    public String getMessage() {
      return message;
    }
  }
}
