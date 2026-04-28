# Abbreviated Tax Invoice PDF Generation Service — Design Spec

**Date:** 2026-04-28  
**Status:** Approved  
**Scope:** `abbreviatedtaxinvoice-pdf-generation-service` (new) + `GENERATE_ABBREVIATED_TAX_INVOICE_PDF` SagaStep in `saga-commons`

---

## 1. Overview

A new Spring Boot microservice that generates PDF/A-3 documents for Thai e-Tax Abbreviated Tax Invoice (`AbbreviatedTaxInvoice_CrossIndustryInvoice`) documents. It participates in the Saga Orchestration pipeline, consuming commands from the orchestrator and replying via the transactional outbox pattern.

The service is a full hexagonal port of `taxinvoice-pdf-generation-service` (port 8089) with all production features. The key structural differences from TaxInvoice are: different XML namespaces, a different root element name, and the absence of a buyer party (abbreviated tax invoices are seller-only). The XSL-FO template renders a full-width seller section instead of a 2-column seller/buyer layout.

---

## 2. Saga Commons Change

**File:** `saga-commons/src/main/java/com/wpanther/saga/domain/enums/SagaStep.java`

Add one new enum value after `GENERATE_RECEIPT_PDF`:

```java
/**
 * Abbreviated tax invoice PDF generation via abbreviatedtaxinvoice-pdf-generation-service.
 */
GENERATE_ABBREVIATED_TAX_INVOICE_PDF("generate-abbreviated-tax-invoice-pdf", "Abbreviated Tax Invoice PDF Generation Service"),
```

---

## 3. Service Identity

| Property | Value |
|----------|-------|
| Port | `8096` |
| Artifact ID | `abbreviatedtaxinvoice-pdf-generation-service` |
| Package root | `com.wpanther.abbreviatedtaxinvoice.pdf` |
| Main class | `AbbreviatedTaxInvoicePdfGenerationServiceApplication` |
| Database | `abbreviatedtaxinvoicepdf_db` (PostgreSQL) |
| MinIO bucket | `abbreviatedtaxinvoices` (env: `MINIO_BUCKET_NAME`) |
| Camel app name | `abbreviatedtaxinvoice-pdf-generation-camel` |

---

## 4. Architecture — Hexagonal (Port/Adapter)

```
com.wpanther.abbreviatedtaxinvoice.pdf/
├── domain/
│   ├── model/
│   │   ├── AbbreviatedTaxInvoicePdfDocument.java   # Aggregate root
│   │   └── GenerationStatus.java                   # PENDING → GENERATING → COMPLETED/FAILED
│   ├── repository/
│   │   └── AbbreviatedTaxInvoicePdfDocumentRepository.java
│   ├── service/
│   │   └── AbbreviatedTaxInvoicePdfGenerationService.java  # Port interface
│   ├── exception/
│   │   └── AbbreviatedTaxInvoicePdfGenerationException.java
│   └── constants/
│       └── PdfGenerationConstants.java
│
├── application/
│   ├── service/
│   │   ├── SagaCommandHandler.java
│   │   └── AbbreviatedTaxInvoicePdfDocumentService.java
│   ├── usecase/
│   │   ├── ProcessAbbreviatedTaxInvoicePdfUseCase.java
│   │   └── CompensateAbbreviatedTaxInvoicePdfUseCase.java
│   └── port/out/
│       ├── PdfEventPort.java
│       ├── PdfStoragePort.java
│       ├── SagaReplyPort.java
│       └── SignedXmlFetchPort.java
│
└── infrastructure/
    ├── adapter/in/kafka/
    │   ├── KafkaAbbreviatedTaxInvoiceProcessCommand.java
    │   ├── KafkaAbbreviatedTaxInvoiceCompensateCommand.java
    │   ├── KafkaCommandMapper.java
    │   └── SagaRouteConfig.java
    ├── adapter/out/
    │   ├── client/
    │   │   └── RestTemplateSignedXmlFetcher.java   # Circuit breaker: signedXmlFetch
    │   ├── messaging/
    │   │   ├── EventPublisher.java
    │   │   ├── SagaReplyPublisher.java
    │   │   ├── AbbreviatedTaxInvoicePdfGeneratedEvent.java
    │   │   ├── AbbreviatedTaxInvoicePdfReplyEvent.java
    │   │   └── OutboxConstants.java
    │   ├── pdf/
    │   │   ├── FopAbbreviatedTaxInvoicePdfGenerator.java
    │   │   ├── PdfA3Converter.java
    │   │   ├── ThaiAmountWordsConverter.java
    │   │   └── AbbreviatedTaxInvoicePdfGenerationServiceImpl.java
    │   ├── persistence/
    │   │   ├── AbbreviatedTaxInvoicePdfDocumentEntity.java
    │   │   ├── JpaAbbreviatedTaxInvoicePdfDocumentRepository.java
    │   │   ├── AbbreviatedTaxInvoicePdfDocumentRepositoryAdapter.java
    │   │   └── outbox/
    │   │       ├── OutboxEventEntity.java
    │   │       ├── SpringDataOutboxRepository.java
    │   │       └── JpaOutboxEventRepository.java
    │   └── storage/
    │       ├── MinioStorageAdapter.java
    │       └── MinioCleanupService.java
    ├── config/
    │   ├── MinioConfig.java
    │   ├── OutboxConfig.java
    │   ├── RestTemplateConfig.java
    │   └── FontHealthCheck.java
    └── metrics/
        └── PdfGenerationMetrics.java
```

---

## 5. Kafka Topics

| Topic | Direction | Description |
|-------|-----------|-------------|
| `saga.command.abbreviated-tax-invoice-pdf` | Consume | Process command from Orchestrator |
| `saga.compensation.abbreviated-tax-invoice-pdf` | Consume | Compensation command from Orchestrator |
| `pdf.generated.abbreviated-tax-invoice` | Produce (outbox) | Notification Service |
| `saga.reply.abbreviated-tax-invoice-pdf` | Produce (outbox) | Reply to Orchestrator |
| `pdf.generation.abbreviated-tax-invoice.dlq` | Produce | Dead letter queue |

Consumer group IDs:
- Command: `abbreviated-tax-invoice-pdf-generation-command`
- Compensation: `abbreviated-tax-invoice-pdf-generation-compensation`

---

## 6. Saga Command/Reply Schema

### Input: `KafkaAbbreviatedTaxInvoiceProcessCommand` (extends `SagaCommand`)

```json
{
  "eventId": "uuid",
  "occurredAt": "...",
  "eventType": "...",
  "version": 1,
  "sagaId": "uuid",
  "sagaStep": "generate-abbreviated-tax-invoice-pdf",
  "correlationId": "uuid",
  "documentId": "uuid",
  "documentNumber": "423-612",
  "signedXmlUrl": "http://document-storage/documents/..."
}
```

### Output: `AbbreviatedTaxInvoicePdfReplyEvent` (extends `SagaReply`)

```json
{
  "sagaId": "uuid",
  "sagaStep": "generate-abbreviated-tax-invoice-pdf",
  "correlationId": "uuid",
  "status": "SUCCESS|FAILURE|COMPENSATED",
  "errorMessage": null,
  "pdfUrl": "http://localhost:9000/abbreviatedtaxinvoices/2026/04/28/abbreviated-tax-invoice-423-612-abc.pdf",
  "pdfSize": 98765
}
```

`pdfUrl` and `pdfSize` present only in SUCCESS replies. Orchestrator stores them in `DocumentMetadata` for the subsequent `PDF_STORAGE` step.

### Output: `AbbreviatedTaxInvoicePdfGeneratedEvent` (outbox → `pdf.generated.abbreviated-tax-invoice`)

```json
{
  "eventId": "uuid",
  "eventType": "pdf.generated.abbreviated-tax-invoice",
  "version": 1,
  "documentId": "uuid",
  "abbreviatedTaxInvoiceId": "uuid",
  "abbreviatedTaxInvoiceNumber": "423-612",
  "documentUrl": "http://...",
  "fileSize": 98765,
  "xmlEmbedded": true,
  "correlationId": "uuid"
}
```

---

## 7. Domain Model — `AbbreviatedTaxInvoicePdfDocument`

Aggregate root with the same state machine as `TaxInvoicePdfDocument`:

| State transition | Method | Guard |
|-----------------|--------|-------|
| `PENDING → GENERATING` | `startGeneration()` | Must be PENDING |
| `GENERATING → COMPLETED` | `markCompleted(path, url, size)` | Must be GENERATING |
| `any → FAILED` | `markFailed(message)` | — |

Key fields: `abbreviatedTaxInvoiceId` (unique constraint / idempotency key), `abbreviatedTaxInvoiceNumber`, `documentPath` (MinIO S3 key), `documentUrl` (full MinIO URL), `retryCount` (max 3).

MinIO S3 key pattern: `YYYY/MM/DD/abbreviated-tax-invoice-{documentNumber}-{uuid}.pdf`

---

## 8. PDF Generation Pipeline

```
KafkaAbbreviatedTaxInvoiceProcessCommand
        ↓
SagaCommandHandler
        ├── Idempotency check (COMPLETED? re-publish and return SUCCESS)
        ├── Retry limit check (retryCount >= maxRetries? send FAILURE)
        └── AbbreviatedTaxInvoicePdfDocumentService.generatePdf()
                ├── Create domain aggregate (PENDING → GENERATING)
                ├── AbbreviatedTaxInvoicePdfGenerationServiceImpl.generatePdf()
                │   ├── RestTemplateSignedXmlFetcher.fetch(signedXmlUrl) → signedXml
                │   ├── XPath on signedXml (AbbreviatedTaxInvoice namespaces)
                │   │     → extract GrandTotalAmount
                │   ├── ThaiAmountWordsConverter.toWords(grandTotal) → amountInWords
                │   ├── FopAbbreviatedTaxInvoicePdfGenerator (amountInWords param) → base PDF
                │   └── PdfA3Converter → PDF/A-3b with embedded XML
                ├── MinioStorageAdapter.store(pdf, key) → pdfUrl
                └── markCompleted() → COMPLETED
        ├── EventPublisher → outbox_events (pdf.generated.abbreviated-tax-invoice)
        └── SagaReplyPublisher → outbox_events (saga.reply.abbreviated-tax-invoice-pdf)
```

### Compensation Flow

```
KafkaAbbreviatedTaxInvoiceCompensateCommand
        ↓
SagaCommandHandler.handleCompensation()
        ├── MinioStorageAdapter.delete(documentPath)
        ├── Delete database record
        └── SagaReplyPublisher → COMPENSATED reply (idempotent if no record)
```

---

## 9. XSL-FO Template (`abbreviatedtaxinvoice-direct.xsl`)

Adapted from `taxinvoice-direct.xsl`. Changes:

| Element | TaxInvoice | AbbreviatedTaxInvoice |
|---------|-----------|----------------------|
| `rsm` namespace URI | `...TaxInvoice_CrossIndustryInvoice:2` | `...AbbreviatedTaxInvoice_CrossIndustryInvoice:2` |
| `ram` namespace URI | `...TaxInvoice_ReusableAggregateBusinessInformationEntity:2` | `...AbbreviatedTaxInvoice_ReusableAggregateBusinessInformationEntity:2` |
| Root match | `/rsm:TaxInvoice_CrossIndustryInvoice` | `/rsm:AbbreviatedTaxInvoice_CrossIndustryInvoice` |
| Document title (Thai) | `ใบเสร็จรับเงิน / ใบกำกับภาษี / RECEIPT / TAX INVOICE` | `ใบเสร็จรับเงิน/ใบกำกับภาษีอย่างย่อ / ABBREVIATED TAX INVOICE` |
| Header subtitle | `e-Tax Tax Invoice / ใบเสร็จรับเงิน/ใบกำกับภาษีอิเล็กทรอนิกส์` | `e-Tax Abbreviated Tax Invoice / ใบกำกับภาษีอย่างย่อ` |
| Party panel | 2-column: Seller + Buyer | Full-width Seller only |
| `$buyer` variable | present | removed |

All XPath paths for `GrandTotalAmount`, line items, and monetary summation are structurally identical — only namespace URIs differ. `amountInWords` XSLT parameter injection unchanged.

XPath for `GrandTotalAmount` (Java side):

```
/rsm:AbbreviatedTaxInvoice_CrossIndustryInvoice
  /rsm:SupplyChainTradeTransaction
  /ram:ApplicableHeaderTradeSettlement
  /ram:SpecifiedTradeSettlementHeaderMonetarySummation
  /ram:GrandTotalAmount
```

Namespace bindings:
- `rsm` → `urn:etda:uncefact:data:standard:AbbreviatedTaxInvoice_CrossIndustryInvoice:2`
- `ram` → `urn:etda:uncefact:data:standard:AbbreviatedTaxInvoice_ReusableAggregateBusinessInformationEntity:2`

Resources copied as-is from taxinvoice: `fop.xconf`, `sRGB.icc`, Thai font files (`THSarabunNew`, `NotoSansThaiLooped`).

---

## 10. Database — `abbreviatedtaxinvoicepdf_db`

Single Flyway migration: `V1__create_abbreviated_tax_invoice_pdf_tables.sql`

Creates in one script:
- `abbreviated_tax_invoice_pdf_documents` table with `abbreviated_tax_invoice_id` unique constraint and `retry_count` column
- `outbox_events` table with compound index on `(status, created_at)`

---

## 11. Configuration (`application.yml`)

All env vars mirror taxinvoice defaults. Abbreviated-tax-invoice-specific values:

| Variable | Default |
|----------|---------|
| `server.port` | `8096` |
| `DB_NAME` | `abbreviatedtaxinvoicepdf_db` |
| `MINIO_BUCKET_NAME` | `abbreviatedtaxinvoices` |
| `KAFKA_COMMAND_GROUP_ID` | `abbreviated-tax-invoice-pdf-generation-command` |
| `KAFKA_COMPENSATION_GROUP_ID` | `abbreviated-tax-invoice-pdf-generation-compensation` |
| `spring.application.name` | `abbreviatedtaxinvoice-pdf-generation-service` |
| `camel.springboot.name` | `abbreviatedtaxinvoice-pdf-generation-camel` |

All other variables (`MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `KAFKA_BROKERS`, `PDF_GENERATION_MAX_RETRIES`, `PDF_MAX_CONCURRENT_RENDERS`, `PDF_MAX_SIZE_BYTES`, `REST_CLIENT_CONNECT_TIMEOUT`, `REST_CLIENT_READ_TIMEOUT`, `FONT_HEALTH_CHECK_ENABLED`, etc.) use the same defaults as taxinvoice.

---

## 12. Testing Strategy

90% JaCoCo line coverage requirement. H2 in-memory DB, Flyway disabled in `application-test.yml`. Simplified `fop.xconf` for tests (no PDF/A mode, auto-detect fonts — no Thai font files required).

| Test class | What it covers |
|------------|---------------|
| `SagaCommandHandlerTest` | success, idempotency, max retries, generation failure, compensation success, idempotent compensation, compensation failure |
| `CamelRouteConfigTest` | JSON serialization/deserialization of all event types |
| `FopAbbreviatedTaxInvoicePdfGeneratorTest` | constructor validation, semaphore, valid/malformed XML, size limit, thread interruption, URI resolution, font availability |
| `PdfA3ConverterTest` | constructor, null/empty PDF, exception constructors |
| `MinioStorageAdapterTest` | upload, delete, URL resolution, Thai chars, filename sanitization |
| `AbbreviatedTaxInvoicePdfDocumentTest` | state machine transitions, invariants, retry counting |
| `AbbreviatedTaxInvoicePdfDocumentServiceTest` | transactional service methods |
| `EventPublisherTest` | outbox event publishing |
| `SagaReplyPublisherTest` | outbox reply publishing |
| `RestTemplateSignedXmlFetcherTest` | REST client with circuit breaker |
| `KafkaCommandMapperTest` | command mapping |
| `MinioCleanupServiceTest` | cleanup scheduling |
| `FontHealthCheckTest` | font validation at startup |

No embedded Kafka integration tests. No REST API — Spring Actuator only (`/actuator/health`, `/actuator/metrics`, `/actuator/camelroutes`, `/actuator/prometheus`).

---

## 13. Out of Scope

- Orchestrator changes (routing `GENERATE_ABBREVIATED_TAX_INVOICE_PDF` commands) — separate task
- `abbreviatedtaxinvoice-processing-service` changes
- Integration tests with embedded Kafka
