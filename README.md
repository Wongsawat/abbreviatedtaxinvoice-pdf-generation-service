# Abbreviated Tax Invoice PDF Generation Service

A Spring Boot microservice for generating PDF/A-3 documents for Thai e-Tax abbreviated tax invoices with embedded XML attachments. This service participates in a Saga Orchestration pattern coordinated by the orchestrator-service.

## Tech Stack

| Component | Version |
|-----------|---------|
| Java | 21 |
| Spring Boot | 3.2.5 |
| Spring Cloud | 2023.0.1 |
| Apache Camel | 4.14.4 |
| Apache FOP | 2.9 |
| PostgreSQL | 16+ |
| Kafka | Latest |

## Features

- **PDF/A-3 Generation** with embedded XML using Apache FOP
- **Saga Orchestration** for coordinated transaction processing
- **Transactional Outbox Pattern** for reliable event publishing
- **Hexagonal Architecture** (Ports & Adapters pattern)
- **Domain-Driven Design** with clean layer separation
- **Idempotent Processing** with automatic retry handling
- **Circuit Breaker** for external service calls (MinIO, signed XML fetch)

## Architecture

This service follows **Hexagonal Architecture** (Ports & Adapters) with Domain-Driven Design:

```
┌─────────────────────────────────────────────────────────────────┐
│                         Infrastructure                          │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │
│  │ Adapter/In  │  │ Adapter/Out │  │    Config   │             │
│  │   (Kafka)   │  │ (PDF, Store,│  │  (Beans)    │             │
│  │             │  │  Client, DB)│  │             │             │
│  └──────┬──────┘  └──────┬──────┘  └─────────────┘             │
└─────────┼──────────────────┼───────────────────────────────────┘
          │                  │
┌─────────┼──────────────────┼───────────────────────────────────┐
│         │      Application │                                  │
│         │   ┌──────────────┴──────────────┐                   │
│         │   │  UseCase (Inbound Ports)    │                   │
│         │   │  Port/Out (Outbound Ports)  │                   │
│         │   └─────────────────────────────┘                   │
└─────────┼──────────────────────────────────────────────────────┘
          │
┌─────────┼──────────────────────────────────────────────────────┐
│         │              Domain                                   │
│         │   ┌─────────────────────────────┐                   │
│         │   │  Model (Aggregates, VOs)     │                   │
│         │   │  Repository (Domain-owned)   │                   │
│         │   │  Service (Domain Services)   │                   │
│         │   └─────────────────────────────┘                   │
└─────────┴──────────────────────────────────────────────────────┘
```

### Layer Responsibilities

| Layer | Purpose | Contents |
|-------|---------|----------|
| **Domain** | Core business rules, zero framework dependencies | Aggregates, value objects, domain services, repository interfaces |
| **Application** | Use case orchestration | Inbound port interfaces (`port/in/`), outbound port interfaces (`port/out/`) |
| **Infrastructure** | External world interactions | Kafka DTOs, PDF generation, storage, persistence, configuration |

## Prerequisites

- **Java 21+**
- **Maven 3.6+**
- **PostgreSQL 16+** with database `abbreviatedtaxinvoicepdf_db`
- **Kafka** on `localhost:9092`
- **MinIO** (S3-compatible storage) on `localhost:9000` with bucket `abbreviatedtaxinvoices`
- **teda library** installed (`cd ../../teda && mvn clean install`)
- **saga-commons library** installed (`cd ../../saga-commons && mvn clean install`)

## Quick Start

```bash
# Build dependencies (teda and saga-commons)
cd ../../teda && mvn clean install
cd ../saga-commons && mvn clean install

# Build and run
cd abbreviatedtaxinvoice-pdf-generation-service
mvn clean package
mvn spring-boot:run
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DB_HOST` | `localhost` | PostgreSQL host |
| `DB_PORT` | `5432` | PostgreSQL port |
| `DB_NAME` | `abbreviatedtaxinvoicepdf_db` | Database name |
| `KAFKA_BROKERS` | `localhost:9092` | Kafka bootstrap servers |
| `KAFKA_CONSUMERS_COUNT` | `3` | Camel concurrent consumers |
| `MINIO_ENDPOINT` | `http://localhost:9000` | MinIO endpoint |
| `MINIO_BUCKET_NAME` | `abbreviatedtaxinvoices` | Target bucket name |
| `PDF_MAX_CONCURRENT_RENDERS` | `3` | Max concurrent PDF generations |
| `PDF_GENERATION_MAX_RETRIES` | `3` | Saga retry limit per document |

### Database Setup

```bash
mvn flyway:migrate
```

## Kafka Topics

### Inbound (Consumed)

| Topic | Purpose |
|-------|---------|
| `saga.command.abbreviated-tax-invoice-pdf` | Process abbreviated tax invoice PDF generation commands |
| `saga.compensation.abbreviated-tax-invoice-pdf` | Compensation commands for rollback |

### Outbound (Produced via Outbox)

| Topic | Purpose |
|-------|---------|
| `saga.reply.abbreviated-tax-invoice-pdf` | SUCCESS/FAILURE/COMPENSATED replies to orchestrator |
| `pdf.generated.abbreviated-tax-invoice` | Notification events for downstream services |
| `document.archive` | Fire-and-forget archival (UNSIGNED_PDF stored by document-storage-service) |

### Command Format

```json
{
  "sagaId": "uuid",
  "sagaStep": "GENERATE_ABBREVIATED_TAX_INVOICE_PDF",
  "correlationId": "uuid",
  "documentId": "uuid",
  "documentNumber": "ABBR-2024-001",
  "signedXmlUrl": "http://localhost:9000/abbreviatedtaxinvoices/signed-xml-key"
}
```

The service downloads signed XML from `signedXmlUrl` at runtime — no inline XML or JSON payload in the command.

## API Endpoints

This service is **event-driven only**. No REST API beyond Spring Actuator:

- `/actuator/health` - Health check
- `/actuator/metrics` - Metrics
- `/actuator/camelroutes` - Camel route information

## Testing

```bash
mvn clean test     # All tests (use 'clean' to avoid Lombok staleness)
mvn verify         # Tests + JaCoCo coverage check
```

## Project Structure

```
src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/
├── domain/
│   ├── model/                    # Aggregate roots, value objects
│   ├── repository/               # Domain-owned repository interfaces
│   ├── service/                  # Domain service interfaces
│   └── exception/                # Domain exceptions with error codes
├── application/
│   ├── port/in/                  # Inbound port interfaces (use cases)
│   └── port/out/                  # Outbound port interfaces
└── infrastructure/
    ├── adapter/
    │   ├── in/kafka/             # Kafka consumer adapters + DTOs
    │   │   ├── dto/               # Kafka command DTOs (SagaCommand + Jackson)
    │   │   ├── SagaCommandHandler.java
    │   │   └── SagaRouteConfig.java
    │   └── out/                  # Output adapters
    │       ├── pdf/              # PDF generation (FOP)
    │       ├── storage/          # MinIO/S3 storage
    │       ├── persistence/      # JPA entities & repositories
    │       ├── messaging/        # Event publishers (outbox pattern)
    │       └── client/           # External HTTP clients
    └── config/                   # Spring configuration
```

## License

This project is licensed under the MIT License.

## Maintainer

**Weerachat Wongsawat**
[ rabbit_roger@yahoo.com ](mailto:rabbit_roger@yahoo.com)
