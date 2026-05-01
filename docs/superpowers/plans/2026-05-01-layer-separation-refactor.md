# abbreviatedtaxinvoice-pdf-generation-service Layer Separation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor abbreviatedtaxinvoice-pdf-generation-service to place saga infrastructure types in `infrastructure/` and use case interfaces with plain parameters in `application/port/in/`, matching the architecture of invoice-pdf-generation-service (refactored 2026-04-30).

**Architecture:** Kafka DTOs (SagaCommand/SagaReply + Jackson) live in `infrastructure/adapter/in/kafka/dto/`. Use case interfaces accept plain field parameters. `SagaCommandHandler` is the driving adapter — it directly implements both port interfaces, extracts fields from Kafka DTOs, and calls use case methods with plain parameters only. No command objects flow into domain or application layers. `AbbreviatedTaxInvoicePdfReplyEvent` factory is inlined in `SagaReplyPublisher`.

**Tech Stack:** Java 21, Spring Boot 3.2.5, Apache Camel 4.14.4, saga-commons library, Jackson

---

## File Changes Overview

```
CREATING:
  infrastructure/adapter/in/kafka/dto/ProcessAbbreviatedTaxInvoicePdfCommand.java
  infrastructure/adapter/in/kafka/dto/CompensateAbbreviatedTaxInvoicePdfCommand.java
  application/port/in/ProcessAbbreviatedTaxInvoicePdfUseCase.java
  application/port/in/CompensateAbbreviatedTaxInvoicePdfUseCase.java

MOVING:
  SagaCommandHandler.java          application/service/ → infrastructure/adapter/in/kafka/

DELETING:
  infrastructure/adapter/in/kafka/KafkaAbbreviatedTaxInvoiceProcessCommand.java
  infrastructure/adapter/in/kafka/KafkaAbbreviatedTaxInvoiceCompensateCommand.java
  infrastructure/adapter/in/kafka/KafkaCommandMapper.java
  infrastructure/adapter/out/messaging/AbbreviatedTaxInvoicePdfReplyEvent.java
  application/usecase/ProcessAbbreviatedTaxInvoicePdfUseCase.java
  application/usecase/CompensateAbbreviatedTaxInvoicePdfUseCase.java
  application/usecase/ProcessAbbreviatedTaxInvoicePdfUseCaseImpl.java
  application/usecase/CompensateAbbreviatedTaxInvoicePdfUseCaseImpl.java
  application/service/SagaCommandHandler.java (original location)

MODIFYING:
  SagaRouteConfig.java             (use new DTOs, remove KafkaCommandMapper, field extraction in .process())
  SagaReplyPublisher.java          (inline AbbreviatedTaxInvoicePdfReplyEvent factory)

UNCHANGED:
  AbbreviatedTaxInvoicePdfGeneratedEvent (already correctly in infrastructure/adapter/out/messaging/)
  EventPublisher.java
  domain/model/, domain/service/, domain/repository/
```

---

## Before You Start

- Build the service to confirm it compiles: `mvn clean compile -q`
- Run tests to confirm baseline: `mvn clean test -q`
- Work inside the service directory: `/home/wpanther/projects/etax/invoice-microservices/services/abbreviatedtaxinvoice-pdf-generation-service`

---

## Task 1: Create `dto/` directory and new `ProcessAbbreviatedTaxInvoicePdfCommand`

**Files:**
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/in/kafka/dto/ProcessAbbreviatedTaxInvoicePdfCommand.java`
- Source: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/in/kafka/KafkaAbbreviatedTaxInvoiceProcessCommand.java`

- [ ] **Step 1: Create directory and new file**

```bash
mkdir -p src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/in/kafka/dto
```

Create `ProcessAbbreviatedTaxInvoicePdfCommand.java`:
```java
package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.in.kafka.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.wpanther.saga.domain.enums.SagaStep;
import com.wpanther.saga.domain.model.SagaCommand;
import lombok.Getter;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

@Getter
public class ProcessAbbreviatedTaxInvoicePdfCommand extends SagaCommand {

    private static final long serialVersionUID = 1L;

    @JsonProperty("documentId")
    private final String documentId;

    @JsonProperty("documentNumber")
    private final String documentNumber;

    @JsonProperty("signedXmlUrl")
    private final String signedXmlUrl;

    @JsonCreator
    public ProcessAbbreviatedTaxInvoicePdfCommand(
            @JsonProperty("eventId") UUID eventId,
            @JsonProperty("occurredAt") Instant occurredAt,
            @JsonProperty("eventType") String eventType,
            @JsonProperty("version") int version,
            @JsonProperty("sagaId") String sagaId,
            @JsonProperty("sagaStep") SagaStep sagaStep,
            @JsonProperty("correlationId") String correlationId,
            @JsonProperty("documentId") String documentId,
            @JsonProperty("documentNumber") String documentNumber,
            @JsonProperty("signedXmlUrl") String signedXmlUrl) {
        super(eventId, occurredAt, eventType, version, sagaId, sagaStep, correlationId);
        this.documentId = documentId;
        this.documentNumber = documentNumber;
        this.signedXmlUrl = signedXmlUrl;
    }

    public ProcessAbbreviatedTaxInvoicePdfCommand(String sagaId, SagaStep sagaStep, String correlationId,
                                                  String documentId, String documentNumber,
                                                  String signedXmlUrl) {
        super(sagaId, sagaStep, correlationId);
        this.documentId = Objects.requireNonNull(documentId, "documentId is required");
        this.documentNumber = Objects.requireNonNull(documentNumber, "documentNumber is required");
        this.signedXmlUrl = Objects.requireNonNull(signedXmlUrl, "signedXmlUrl is required");
    }
}
```

- [ ] **Step 2: Verify it compiles**

Run: `mvn compile -q 2>&1 | head -20`
Expected: No errors related to the new file

- [ ] **Step 3: Commit**

```bash
git add src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/in/kafka/dto/ProcessAbbreviatedTaxInvoicePdfCommand.java
git commit -m "refactor: add ProcessAbbreviatedTaxInvoicePdfCommand in dto/ package"
```

---

## Task 2: Create `CompensateAbbreviatedTaxInvoicePdfCommand` in `dto/`

**Files:**
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/in/kafka/dto/CompensateAbbreviatedTaxInvoicePdfCommand.java`
- Source: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/in/kafka/KafkaAbbreviatedTaxInvoiceCompensateCommand.java`

- [ ] **Step 1: Write the new file**

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.in.kafka.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.wpanther.saga.domain.enums.SagaStep;
import com.wpanther.saga.domain.model.SagaCommand;
import lombok.Getter;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

@Getter
public class CompensateAbbreviatedTaxInvoicePdfCommand extends SagaCommand {

    private static final long serialVersionUID = 1L;

    @JsonProperty("documentId")
    private final String documentId;

    @JsonCreator
    public CompensateAbbreviatedTaxInvoicePdfCommand(
            @JsonProperty("eventId") UUID eventId,
            @JsonProperty("occurredAt") Instant occurredAt,
            @JsonProperty("eventType") String eventType,
            @JsonProperty("version") int version,
            @JsonProperty("sagaId") String sagaId,
            @JsonProperty("sagaStep") SagaStep sagaStep,
            @JsonProperty("correlationId") String correlationId,
            @JsonProperty("documentId") String documentId) {
        super(eventId, occurredAt, eventType, version, sagaId, sagaStep, correlationId);
        this.documentId = Objects.requireNonNull(documentId, "documentId is required");
    }

    public CompensateAbbreviatedTaxInvoicePdfCommand(String sagaId, SagaStep sagaStep, String correlationId,
                                                     String documentId) {
        super(sagaId, sagaStep, correlationId);
        this.documentId = Objects.requireNonNull(documentId, "documentId is required");
    }
}
```

- [ ] **Step 2: Verify it compiles**

Run: `mvn compile -q 2>&1 | head -20`
Expected: No errors

- [ ] **Step 3: Commit**

```bash
git add src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/in/kafka/dto/CompensateAbbreviatedTaxInvoicePdfCommand.java
git commit -m "refactor: add CompensateAbbreviatedTaxInvoicePdfCommand in dto/ package"
```

---

## Task 3: Create `ProcessAbbreviatedTaxInvoicePdfUseCase` in `application/port/in/`

**Files:**
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/application/port/in/ProcessAbbreviatedTaxInvoicePdfUseCase.java`

- [ ] **Step 1: Write the new interface**

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.application.port.in;

import com.wpanther.saga.domain.enums.SagaStep;

/**
 * Inbound port for abbreviated tax invoice PDF generation.
 * Called by SagaCommandHandler (in infrastructure) with plain fields — no command objects.
 */
public interface ProcessAbbreviatedTaxInvoicePdfUseCase {

    void handle(String documentId, String documentNumber, String signedXmlUrl,
                String sagaId, SagaStep sagaStep, String correlationId);
}
```

- [ ] **Step 2: Verify it compiles**

Run: `mvn compile -q 2>&1 | head -20`
Expected: No errors

- [ ] **Step 3: Commit**

```bash
git add src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/application/port/in/ProcessAbbreviatedTaxInvoicePdfUseCase.java
git commit -m "refactor: add ProcessAbbreviatedTaxInvoicePdfUseCase in application/port/in/ with plain parameter signatures"
```

---

## Task 4: Create `CompensateAbbreviatedTaxInvoicePdfUseCase` in `application/port/in/`

**Files:**
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/application/port/in/CompensateAbbreviatedTaxInvoicePdfUseCase.java`

- [ ] **Step 1: Write the new interface**

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.application.port.in;

import com.wpanther.saga.domain.enums.SagaStep;

/**
 * Inbound port for abbreviated tax invoice PDF compensation.
 * Called by SagaCommandHandler (in infrastructure) with plain fields — no command objects.
 */
public interface CompensateAbbreviatedTaxInvoicePdfUseCase {

    void handle(String documentId, String sagaId, SagaStep sagaStep, String correlationId);
}
```

- [ ] **Step 2: Verify it compiles**

Run: `mvn compile -q 2>&1 | head -20`
Expected: No errors

- [ ] **Step 3: Commit**

```bash
git add src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/application/port/in/CompensateAbbreviatedTaxInvoicePdfUseCase.java
git commit -m "refactor: add CompensateAbbreviatedTaxInvoicePdfUseCase in application/port/in/ with plain parameter signatures"
```

---

## Task 5: Rewrite `SagaCommandHandler` in `infrastructure/adapter/in/kafka/`

**Files:**
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/in/kafka/SagaCommandHandler.java` (new location)
- Delete: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/application/service/SagaCommandHandler.java` (later)

This is the most complex rewrite. The handler:
1. Is annotated `@Service`
2. Implements both `ProcessAbbreviatedTaxInvoicePdfUseCase` and `CompensateAbbreviatedTaxInvoicePdfUseCase`
3. Receives plain parameters (not command objects) from `SagaRouteConfig`
4. Contains all saga orchestration logic (retry tracking, error handling, compensation)

**Key differences from invoice-pdf-generation-service reference:**
- No `AbbreviatedTaxInvoicePdfDocumentService` exists — the handler directly uses `AbbreviatedTaxInvoicePdfDocumentRepository`
- `AbbreviatedTaxInvoicePdfGenerationService` is in `domain/service/`
- `SignedXmlFetchException` from `RestTemplateSignedXmlFetcher` is used for error handling
- Saga step is `GENERATE_ABBREVIATED_TAX_INVOICE_PDF`

- [ ] **Step 1: Write the new SagaCommandHandler**

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.in.kafka;

import com.wpanther.abbreviatedtaxinvoice.pdf.application.port.in.CompensateAbbreviatedTaxInvoicePdfUseCase;
import com.wpanther.abbreviatedtaxinvoice.pdf.application.port.in.ProcessAbbreviatedTaxInvoicePdfUseCase;
import com.wpanther.abbreviatedtaxinvoice.pdf.application.port.out.PdfEventPort;
import com.wpanther.abbreviatedtaxinvoice.pdf.application.port.out.PdfStoragePort;
import com.wpanther.abbreviatedtaxinvoice.pdf.application.port.out.SagaReplyPort;
import com.wpanther.abbreviatedtaxinvoice.pdf.application.port.out.SignedXmlFetchPort;
import com.wpanther.abbreviatedtaxinvoice.pdf.domain.model.AbbreviatedTaxInvoicePdfDocument;
import com.wpanther.abbreviatedtaxinvoice.pdf.domain.repository.AbbreviatedTaxInvoicePdfDocumentRepository;
import com.wpanther.abbreviatedtaxinvoice.pdf.domain.service.AbbreviatedTaxInvoicePdfGenerationService;
import com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.client.RestTemplateSignedXmlFetcher;
import com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.messaging.AbbreviatedTaxInvoicePdfGeneratedEvent;
import com.wpanther.saga.domain.enums.SagaStep;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class SagaCommandHandler implements ProcessAbbreviatedTaxInvoicePdfUseCase, CompensateAbbreviatedTaxInvoicePdfUseCase {

    private final AbbreviatedTaxInvoicePdfDocumentRepository documentRepository;
    private final AbbreviatedTaxInvoicePdfGenerationService pdfGenerationService;
    private final SignedXmlFetchPort signedXmlFetchPort;
    private final PdfStoragePort pdfStoragePort;
    private final PdfEventPort pdfEventPort;
    private final SagaReplyPort sagaReplyPort;
    private final Counter successCounter;
    private final Counter failureCounter;

    public SagaCommandHandler(
            AbbreviatedTaxInvoicePdfDocumentRepository documentRepository,
            AbbreviatedTaxInvoicePdfGenerationService pdfGenerationService,
            SignedXmlFetchPort signedXmlFetchPort,
            PdfStoragePort pdfStoragePort,
            PdfEventPort pdfEventPort,
            SagaReplyPort sagaReplyPort,
            MeterRegistry meterRegistry) {
        this.documentRepository = documentRepository;
        this.pdfGenerationService = pdfGenerationService;
        this.signedXmlFetchPort = signedXmlFetchPort;
        this.pdfStoragePort = pdfStoragePort;
        this.pdfEventPort = pdfEventPort;
        this.sagaReplyPort = sagaReplyPort;
        this.successCounter = meterRegistry.counter("pdf.generation.success");
        this.failureCounter = meterRegistry.counter("pdf.generation.failure");
    }

    @Override
    public void handle(String documentId, String documentNumber, String signedXmlUrl,
                       String sagaId, SagaStep sagaStep, String correlationId) {
        log.info("Handling saga command for saga: {}, document: {}", sagaId, documentNumber);

        Optional<AbbreviatedTaxInvoicePdfDocument> existingDoc =
                documentRepository.findByAbbreviatedTaxInvoiceId(documentId);

        AbbreviatedTaxInvoicePdfDocument document = existingDoc.orElseGet(() ->
                new AbbreviatedTaxInvoicePdfDocument(documentId, documentNumber));

        try {
            document.startGeneration();
            documentRepository.save(document);

            String xml = signedXmlFetchPort.fetch(signedXmlUrl);
            log.debug("Fetched signed XML: {} bytes", xml.length());

            byte[] pdfBytes = pdfGenerationService.generatePdf(
                    document.getAbbreviatedTaxInvoiceNumber(), xml);

            String s3Key = pdfStoragePort.store(document.getAbbreviatedTaxInvoiceNumber(), pdfBytes);
            String pdfUrl = pdfStoragePort.resolveUrl(s3Key);

            document.markCompleted(s3Key, pdfUrl, (long) pdfBytes.length);
            documentRepository.save(document);

            pdfEventPort.publishPdfGenerated(AbbreviatedTaxInvoicePdfGeneratedEvent.success(
                    document.getId().toString(),
                    document.getAbbreviatedTaxInvoiceId(),
                    document.getAbbreviatedTaxInvoiceNumber(),
                    pdfUrl,
                    (long) pdfBytes.length
            ));

            sagaReplyPort.publishSuccess(
                    sagaId,
                    SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF,
                    correlationId,
                    pdfUrl,
                    (long) pdfBytes.length
            );

            successCounter.increment();
            log.info("PDF generation completed for document: {}", document.getAbbreviatedTaxInvoiceNumber());

        } catch (RestTemplateSignedXmlFetcher.SignedXmlFetchException e) {
            log.error("Failed to fetch signed XML for document: {}", documentNumber, e);
            handleFailure(document, sagaId, correlationId, e, "Failed to fetch signed XML: " + e.getMessage());
        } catch (AbbreviatedTaxInvoicePdfGenerationService.AbbreviatedTaxInvoicePdfGenerationException e) {
            log.error("PDF generation failed for document: {}", documentNumber, e);
            handleFailure(document, sagaId, correlationId, e, "PDF generation failed: " + e.getMessage());
        } catch (Exception e) {
            log.error("Unexpected error during PDF generation for document: {}", documentNumber, e);
            handleFailure(document, sagaId, correlationId, e, "Unexpected error: " + e.getMessage());
        }
    }

    private void handleFailure(AbbreviatedTaxInvoicePdfDocument document,
                               String sagaId, String correlationId,
                               Throwable cause, String errorMessage) {
        try {
            document.incrementRetry();
            if (document.hasExceededMaxRetries()) {
                document.markFailed(errorMessage);
                log.error("PDF generation exhausted retries for document: {}, failing permanently",
                        document.getAbbreviatedTaxInvoiceNumber());
            } else {
                log.warn("PDF generation failed for document: {}, will retry (attempt {})",
                        document.getAbbreviatedTaxInvoiceNumber(), document.getRetryCount());
            }
            documentRepository.save(document);

            sagaReplyPort.publishFailure(
                    sagaId,
                    SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF,
                    correlationId,
                    errorMessage
            );

            failureCounter.increment();
        } catch (Exception e) {
            log.error("Failed to handle failure for document: {}", document.getAbbreviatedTaxInvoiceId(), e);
        }
    }

    @Override
    public void handle(String documentId, String sagaId, SagaStep sagaStep, String correlationId) {
        log.info("Handling compensation for saga: {}, document: {}", sagaId, documentId);

        try {
            Optional<AbbreviatedTaxInvoicePdfDocument> docOpt =
                    documentRepository.findByAbbreviatedTaxInvoiceId(documentId);

            if (docOpt.isPresent()) {
                AbbreviatedTaxInvoicePdfDocument document = docOpt.get();
                if (document.getDocumentPath() != null) {
                    pdfStoragePort.delete(document.getDocumentPath());
                    log.info("Deleted PDF from storage: {}", document.getDocumentPath());
                }
                document.markCompensated();
                documentRepository.save(document);
            }

            sagaReplyPort.publishCompensated(
                    sagaId,
                    SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF,
                    correlationId
            );

            log.info("Compensation completed for document: {}", documentId);

        } catch (Exception e) {
            log.error("Compensation failed for document: {}", documentId, e);
            sagaReplyPort.publishFailure(
                    sagaId,
                    SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF,
                    correlationId,
                    "Compensation failed: " + e.getMessage()
            );
        }
    }

    public void publishOrchestrationFailure(String sagaId, String correlationId, String documentId, String documentNumber, Throwable cause) {
        sagaReplyPort.publishFailure(
                sagaId,
                SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF,
                correlationId,
                cause.getMessage()
        );
    }

    public void publishCompensationOrchestrationFailure(String sagaId, String correlationId, String documentId, Throwable cause) {
        sagaReplyPort.publishFailure(
                sagaId,
                SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF,
                correlationId,
                cause.getMessage()
        );
    }

    public void publishOrchestrationFailureForUnparsedMessage(String sagaId, SagaStep sagaStep, String correlationId, Throwable cause) {
        sagaReplyPort.publishFailure(sagaId, sagaStep, correlationId, cause.getMessage());
    }
}
```

Note: `handle(String, String, SagaStep, String)` for compensation has the same signature as `handle(String, String, String, String, SagaStep, String)` for process — Java will distinguish them by parameter count.

- [ ] **Step 2: Compile and fix any errors**

Run: `mvn compile -q 2>&1 | head -40`
Expected: The new file compiles. Errors may appear in other files referencing the old `SagaCommandHandler` — those are expected and will be fixed in subsequent tasks.

- [ ] **Step 3: Commit**

```bash
git add src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/in/kafka/SagaCommandHandler.java
git commit -m "refactor: rewrite SagaCommandHandler in infrastructure/adapter/in/kafka/ implementing port interfaces with plain parameters"
```

---

## Task 6: Inline `AbbreviatedTaxInvoicePdfReplyEvent` factory in `SagaReplyPublisher`

**Files:**
- Modify: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/messaging/SagaReplyPublisher.java`
- Delete: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/messaging/AbbreviatedTaxInvoicePdfReplyEvent.java` (after inlining)

- [ ] **Step 1: Rewrite SagaReplyPublisher to inline the reply event factory**

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wpanther.abbreviatedtaxinvoice.pdf.application.port.out.SagaReplyPort;
import com.wpanther.saga.domain.enums.ReplyStatus;
import com.wpanther.saga.domain.enums.SagaStep;
import com.wpanther.saga.domain.model.SagaReply;
import com.wpanther.saga.infrastructure.outbox.OutboxService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Component
public class SagaReplyPublisher implements SagaReplyPort {

    private static final Logger log = LoggerFactory.getLogger(SagaReplyPublisher.class);

    private final OutboxService outboxService;
    private final ObjectMapper objectMapper;

    public SagaReplyPublisher(OutboxService outboxService, ObjectMapper objectMapper) {
        this.outboxService = outboxService;
        this.objectMapper = objectMapper;
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void publishSuccess(String sagaId, SagaStep sagaStep, String correlationId, String pdfUrl, long pdfSize) {
        publish(AbbreviatedTaxInvoicePdfReplyEvent.success(sagaId, sagaStep, correlationId, pdfUrl, pdfSize), sagaId);
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void publishFailure(String sagaId, SagaStep sagaStep, String correlationId, String errorMessage) {
        publish(AbbreviatedTaxInvoicePdfReplyEvent.failure(sagaId, sagaStep, correlationId, errorMessage), sagaId);
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void publishCompensated(String sagaId, SagaStep sagaStep, String correlationId) {
        publish(AbbreviatedTaxInvoicePdfReplyEvent.compensated(sagaId, sagaStep, correlationId), sagaId);
    }

    private void publish(AbbreviatedTaxInvoicePdfReplyEvent event, String sagaId) {
        try {
            outboxService.saveWithRouting(
                    event,
                    OutboxConstants.AGGREGATE_TYPE,
                    sagaId,
                    OutboxConstants.TOPIC_SAGA_REPLY,
                    sagaId,
                    "{}"
            );
            log.info("Published saga reply: {} for saga: {}", event.getStatus(), sagaId);
        } catch (Exception e) {
            throw new RuntimeException("Failed to publish AbbreviatedTaxInvoicePdfReplyEvent", e);
        }
    }

    // Inline factory — replaces standalone AbbreviatedTaxInvoicePdfReplyEvent.java
    private static class AbbreviatedTaxInvoicePdfReplyEvent extends SagaReply {

        private static final long serialVersionUID = 1L;

        private final String pdfUrl;
        private final Long pdfSize;
        private final String errorMessage;

        private AbbreviatedTaxInvoicePdfReplyEvent(
                String sagaId,
                SagaStep sagaStep,
                String correlationId,
                ReplyStatus replyStatus,
                String pdfUrl,
                Long pdfSize,
                String errorMessage) {
            super(sagaId, sagaStep, correlationId, replyStatus);
            this.pdfUrl = pdfUrl;
            this.pdfSize = pdfSize;
            this.errorMessage = errorMessage;
        }

        private AbbreviatedTaxInvoicePdfReplyEvent(String sagaId, SagaStep sagaStep, String correlationId, String errorMessage) {
            super(sagaId, sagaStep, correlationId, errorMessage);
            this.pdfUrl = null;
            this.pdfSize = null;
            this.errorMessage = errorMessage;
        }

        public static AbbreviatedTaxInvoicePdfReplyEvent success(
                String sagaId, SagaStep sagaStep, String correlationId, String pdfUrl, Long pdfSize) {
            return new AbbreviatedTaxInvoicePdfReplyEvent(
                    sagaId, sagaStep, correlationId, ReplyStatus.SUCCESS, pdfUrl, pdfSize, null);
        }

        public static AbbreviatedTaxInvoicePdfReplyEvent failure(
                String sagaId, SagaStep sagaStep, String correlationId, String errorMessage) {
            return new AbbreviatedTaxInvoicePdfReplyEvent(
                    sagaId, sagaStep, correlationId, errorMessage);
        }

        public static AbbreviatedTaxInvoicePdfReplyEvent compensated(
                String sagaId, SagaStep sagaStep, String correlationId) {
            return new AbbreviatedTaxInvoicePdfReplyEvent(
                    sagaId, sagaStep, correlationId, ReplyStatus.COMPENSATED, null, null, null);
        }

        @Override
        public String getEventType() {
            return "saga.reply.abbreviated-tax-invoice-pdf";
        }

        public boolean isSuccess() { return ReplyStatus.SUCCESS.equals(getStatus()); }
        public boolean isFailure() { return ReplyStatus.FAILURE.equals(getStatus()); }
        public boolean isCompensated() { return ReplyStatus.COMPENSATED.equals(getStatus()); }
    }
}
```

- [ ] **Step 2: Compile**

Run: `mvn compile -q 2>&1 | head -40`
Expected: Should compile cleanly

- [ ] **Step 3: Commit**

```bash
git add src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/messaging/SagaReplyPublisher.java
git commit -m "refactor: inline AbbreviatedTaxInvoicePdfReplyEvent factory in SagaReplyPublisher"
```

---

## Task 7: Update `SagaRouteConfig` — remove `KafkaCommandMapper`, use new DTOs

**Files:**
- Modify: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/in/kafka/SagaRouteConfig.java`

Key changes:
1. Remove `KafkaCommandMapper` field and constructor parameter
2. Change Kafka DTO class references to new `dto/` package classes
3. Route processors extract DTO fields and pass **plain parameters** to the port interfaces
4. `onPrepareFailure` block uses new DTO class names and calls `sagaCommandHandler` methods with plain parameters

- [ ] **Step 1: Rewrite SagaRouteConfig**

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.in.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wpanther.abbreviatedtaxinvoice.pdf.application.port.in.CompensateAbbreviatedTaxInvoicePdfUseCase;
import com.wpanther.abbreviatedtaxinvoice.pdf.application.port.in.ProcessAbbreviatedTaxInvoicePdfUseCase;
import com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.in.kafka.dto.CompensateAbbreviatedTaxInvoicePdfCommand;
import com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.in.kafka.dto.ProcessAbbreviatedTaxInvoicePdfCommand;
import com.wpanther.saga.domain.enums.SagaStep;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
@Slf4j
public class SagaRouteConfig extends RouteBuilder {

    private final ProcessAbbreviatedTaxInvoicePdfUseCase processUseCase;
    private final CompensateAbbreviatedTaxInvoicePdfUseCase compensateUseCase;
    private final SagaCommandHandler sagaCommandHandler;
    private final ObjectMapper objectMapper;

    public SagaRouteConfig(ProcessAbbreviatedTaxInvoicePdfUseCase processUseCase,
                           CompensateAbbreviatedTaxInvoicePdfUseCase compensateUseCase,
                           SagaCommandHandler sagaCommandHandler,
                           ObjectMapper objectMapper) {
        this.processUseCase = processUseCase;
        this.compensateUseCase = compensateUseCase;
        this.sagaCommandHandler = sagaCommandHandler;
        this.objectMapper = objectMapper;
    }

    @Override
    public void configure() {

        errorHandler(deadLetterChannel(
                        "kafka:{{app.kafka.topics.dlq}}?brokers={{app.kafka.bootstrap-servers}}")
                        .maximumRedeliveries(3)
                        .redeliveryDelay(1000)
                        .useExponentialBackOff()
                        .backOffMultiplier(2)
                        .maximumRedeliveryDelay(10000)
                        .logExhausted(true)
                        .logStackTrace(true)
                        .onPrepareFailure(exchange -> {
                            Throwable cause = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class);
                            Object body = exchange.getIn().getBody();
                            if (body instanceof ProcessAbbreviatedTaxInvoicePdfCommand cmd) {
                                log.error("DLQ: notifying orchestrator of retry exhaustion for saga {} document {}",
                                        cmd.getSagaId(), cmd.getDocumentNumber());
                                sagaCommandHandler.publishOrchestrationFailure(
                                        cmd.getSagaId(), cmd.getCorrelationId(),
                                        cmd.getDocumentId(), cmd.getDocumentNumber(), cause);
                            } else if (body instanceof CompensateAbbreviatedTaxInvoicePdfCommand cmd) {
                                log.error("DLQ: notifying orchestrator of compensation retry exhaustion for saga {} document {}",
                                        cmd.getSagaId(), cmd.getDocumentId());
                                sagaCommandHandler.publishCompensationOrchestrationFailure(
                                        cmd.getSagaId(), cmd.getCorrelationId(), cmd.getDocumentId(), cause);
                            } else {
                                log.error("DLQ: body not deserialized ({}); attempting saga metadata recovery",
                                        body == null ? "null" : body.getClass().getSimpleName());
                                recoverAndNotifyOrchestrator(body, cause);
                            }
                        }));

        from("kafka:{{app.kafka.topics.saga-command-abbreviated-tax-invoice-pdf}}"
                        + "?brokers={{app.kafka.bootstrap-servers}}"
                        + "&groupId={{app.kafka.consumer.command-group-id}}"
                        + "&autoOffsetReset=earliest"
                        + "&autoCommitEnable=false"
                        + "&breakOnFirstError={{app.kafka.consumer.break-on-first-error:true}}"
                        + "&maxPollRecords={{app.kafka.consumer.max-poll-records:100}}"
                        + "&consumersCount={{app.kafka.consumer.consumers-count:3}}")
                .routeId("saga-command-consumer")
                .log(LoggingLevel.DEBUG, "Received saga command from Kafka: partition=${header[kafka.PARTITION]}, offset=${header[kafka.OFFSET]}")
                .unmarshal().json(JsonLibrary.Jackson, ProcessAbbreviatedTaxInvoicePdfCommand.class)
                .process(exchange -> {
                    ProcessAbbreviatedTaxInvoicePdfCommand cmd =
                            exchange.getIn().getBody(ProcessAbbreviatedTaxInvoicePdfCommand.class);
                    log.info("Processing saga command for saga: {}, document: {}",
                            cmd.getSagaId(), cmd.getDocumentNumber());
                    processUseCase.handle(
                            cmd.getDocumentId(),
                            cmd.getDocumentNumber(),
                            cmd.getSignedXmlUrl(),
                            cmd.getSagaId(),
                            cmd.getSagaStep(),
                            cmd.getCorrelationId());
                })
                .log("Successfully processed saga command");

        from("kafka:{{app.kafka.topics.saga-compensation-abbreviated-tax-invoice-pdf}}"
                        + "?brokers={{app.kafka.bootstrap-servers}}"
                        + "&groupId={{app.kafka.consumer.compensation-group-id}}"
                        + "&autoOffsetReset=earliest"
                        + "&autoCommitEnable=false"
                        + "&breakOnFirstError={{app.kafka.consumer.break-on-first-error:true}}"
                        + "&maxPollRecords={{app.kafka.consumer.max-poll-records:100}}"
                        + "&consumersCount={{app.kafka.consumer.consumers-count:3}}")
                .routeId("saga-compensation-consumer")
                .log(LoggingLevel.DEBUG, "Received compensation command from Kafka: partition=${header[kafka.PARTITION]}, offset=${header[kafka.OFFSET]}")
                .unmarshal().json(JsonLibrary.Jackson, CompensateAbbreviatedTaxInvoicePdfCommand.class)
                .process(exchange -> {
                    CompensateAbbreviatedTaxInvoicePdfCommand cmd =
                            exchange.getIn().getBody(CompensateAbbreviatedTaxInvoicePdfCommand.class);
                    log.info("Processing compensation for saga: {}, document: {}",
                            cmd.getSagaId(), cmd.getDocumentId());
                    compensateUseCase.handle(
                            cmd.getDocumentId(),
                            cmd.getSagaId(),
                            cmd.getSagaStep(),
                            cmd.getCorrelationId());
                })
                .log("Successfully processed compensation command");
    }

    private void recoverAndNotifyOrchestrator(Object body, Throwable cause) {
        if (body == null) {
            log.error("DLQ: null message body — orchestrator must timeout");
            return;
        }
        try {
            byte[] rawBytes = body instanceof byte[] b
                    ? b : body.toString().getBytes(StandardCharsets.UTF_8);
            JsonNode node = objectMapper.readTree(rawBytes);
            String sagaId = node.path("sagaId").asText(null);
            String sagaStepStr = node.path("sagaStep").asText(null);
            String correlationId = node.path("correlationId").asText(null);
            if (sagaId == null || sagaStepStr == null) {
                log.error("DLQ: saga metadata missing in raw message — orchestrator must timeout");
                return;
            }
            SagaStep sagaStep = objectMapper.readValue("\"" + sagaStepStr + "\"", SagaStep.class);
            sagaCommandHandler.publishOrchestrationFailureForUnparsedMessage(sagaId, sagaStep, correlationId, cause);
        } catch (Exception e) {
            log.error("DLQ: cannot parse raw message for saga metadata — orchestrator must timeout", e);
        }
    }
}
```

Also add the missing import at the top:
```java
import org.apache.camel.LoggingLevel;
```

- [ ] **Step 2: Compile**

Run: `mvn compile -q 2>&1 | head -40`
Expected: Should compile cleanly

- [ ] **Step 3: Commit**

```bash
git add src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/in/kafka/SagaRouteConfig.java
git commit -m "refactor: update SagaRouteConfig to use new DTO package and remove KafkaCommandMapper"
```

---

## Task 8: Delete old files

**Files:**
- Delete: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/in/kafka/KafkaAbbreviatedTaxInvoiceProcessCommand.java`
- Delete: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/in/kafka/KafkaAbbreviatedTaxInvoiceCompensateCommand.java`
- Delete: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/in/kafka/KafkaCommandMapper.java`
- Delete: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/messaging/AbbreviatedTaxInvoicePdfReplyEvent.java`

- [ ] **Step 1: Delete all old files**

```bash
rm src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/in/kafka/KafkaAbbreviatedTaxInvoiceProcessCommand.java
rm src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/in/kafka/KafkaAbbreviatedTaxInvoiceCompensateCommand.java
rm src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/in/kafka/KafkaCommandMapper.java
rm src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/messaging/AbbreviatedTaxInvoicePdfReplyEvent.java
```

- [ ] **Step 2: Compile**

Run: `mvn compile -q 2>&1 | head -40`
Expected: Clean compile

- [ ] **Step 3: Commit**

```bash
git add -A  # stage deletions
git commit -m "refactor: delete old Kafka command files, KafkaCommandMapper, and standalone AbbreviatedTaxInvoicePdfReplyEvent"
```

---

## Task 9: Delete `application/usecase/` and original `application/service/SagaCommandHandler`

**Files:**
- Delete: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/application/usecase/ProcessAbbreviatedTaxInvoicePdfUseCase.java`
- Delete: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/application/usecase/CompensateAbbreviatedTaxInvoicePdfUseCase.java`
- Delete: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/application/usecase/ProcessAbbreviatedTaxInvoicePdfUseCaseImpl.java`
- Delete: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/application/usecase/CompensateAbbreviatedTaxInvoicePdfUseCaseImpl.java`
- Delete: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/application/service/SagaCommandHandler.java`

```bash
rm src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/application/usecase/ProcessAbbreviatedTaxInvoicePdfUseCase.java
rm src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/application/usecase/CompensateAbbreviatedTaxInvoicePdfUseCase.java
rm src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/application/usecase/ProcessAbbreviatedTaxInvoicePdfUseCaseImpl.java
rm src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/application/usecase/CompensateAbbreviatedTaxInvoicePdfUseCaseImpl.java
rm src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/application/service/SagaCommandHandler.java
```

- [ ] **Step 2: Compile**

Run: `mvn compile -q 2>&1 | head -40`
Expected: Clean compile

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "refactor: delete original SagaCommandHandler and old usecase interfaces (now in application/port/in/)"
```

---

## Task 10: Build and test

**Files:**
- All modified files

- [ ] **Step 1: Full clean compile**

Run: `mvn clean compile 2>&1 | tail -20`
Expected: BUILD SUCCESS

- [ ] **Step 2: Run tests**

Run: `mvn clean test 2>&1 | tail -30`
Expected: BUILD SUCCESS (all tests pass)

- [ ] **Step 3: Commit all remaining changes**

```bash
git add -A
git commit -m "refactor: complete layer separation — saga types in infrastructure, plain-parameter use cases in application/port/in/"
```

---

## Verification Checklist

After all tasks:

```bash
# 1. Compile check
mvn clean compile -q

# 2. All tests pass
mvn clean test -q

# 3. Confirm new structure
ls src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/in/kafka/dto/
# Expected: CompensateAbbreviatedTaxInvoicePdfCommand.java, ProcessAbbreviatedTaxInvoicePdfCommand.java

ls src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/application/port/in/
# Expected: CompensateAbbreviatedTaxInvoicePdfUseCase.java, ProcessAbbreviatedTaxInvoicePdfUseCase.java

ls src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/in/kafka/
# Expected: SagaCommandHandler.java, SagaRouteConfig.java (NO KafkaCommandMapper, NO Kafka*Command files)

ls src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/application/usecase/
# Expected: (empty or directory removed — all UseCaseImpl files deleted)

ls src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/application/service/
# Expected: (empty — SagaCommandHandler moved out)

# 4. Confirm no standalone reply event file remains
ls src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/messaging/
# Expected: AbbreviatedTaxInvoicePdfGeneratedEvent.java, EventPublisher.java, SagaReplyPublisher.java (NO AbbreviatedTaxInvoicePdfReplyEvent.java)
```

---

## Self-Review Checklist

- [ ] `ProcessAbbreviatedTaxInvoicePdfCommand` and `CompensateAbbreviatedTaxInvoicePdfCommand` are in `infrastructure/adapter/in/kafka/dto/`
- [ ] Use case interfaces in `application/port/in/` have plain parameter signatures (no command objects)
- [ ] `SagaCommandHandler` is in `infrastructure/adapter/in/kafka/` and implements both port interfaces
- [ ] `AbbreviatedTaxInvoicePdfReplyEvent` is inlined in `SagaReplyPublisher`
- [ ] `KafkaCommandMapper` is deleted
- [ ] All Kafka-prefixed DTOs are deleted (`KafkaAbbreviatedTaxInvoiceProcessCommand`, `KafkaAbbreviatedTaxInvoiceCompensateCommand`)
- [ ] All `application/usecase/` files deleted (including `*UseCaseImpl`)
- [ ] `application/service/SagaCommandHandler` (original location) is deleted
- [ ] `AbbreviatedTaxInvoicePdfGeneratedEvent` is still in `infrastructure/adapter/out/messaging/` (unchanged, already correct)
- [ ] All tests pass with `mvn clean test`
- [ ] Compilation succeeds with `mvn clean compile`