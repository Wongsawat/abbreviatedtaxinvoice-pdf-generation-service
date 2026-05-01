# abbreviatedtaxinvoice-pdf-generation-service Layer Separation Refactoring

**Date:** 2026-05-01
**Author:** Claude Code
**Status:** Draft

## Context

`abbreviatedtaxinvoice-pdf-generation-service` currently has saga infrastructure types (`KafkaAbbreviatedTaxInvoiceProcessCommand`, `KafkaAbbreviatedTaxInvoiceCompensateCommand`) and `KafkaCommandMapper` (no-op) in `infrastructure/adapter/in/kafka/`, but the use case interfaces in `application/usecase/` accept these Kafka command objects directly — violating Hexagonal/Port-Adapters principles. The domain layer should have no framework or infrastructure dependencies.

The reference implementation is `invoice-pdf-generation-service` (refactored 2026-04-30), which correctly separates:
- Kafka DTOs with saga inheritance and Jackson annotations → `infrastructure/adapter/in/kafka/dto/`
- Use case interfaces with plain parameters → `application/port/in/`
- `SagaCommandHandler` in `infrastructure/adapter/in/kafka/` (driving adapter)
- Reply event factories inlined in `SagaReplyPublisher`

## Problem Statement

Three architectural violations in the current service:

| Location | Violation |
|----------|-----------|
| `application/usecase/ProcessAbbreviatedTaxInvoicePdfUseCase` | `handle(KafkaAbbreviatedTaxInvoiceProcessCommand)` — accepts Kafka DTO, leaks infrastructure |
| `application/usecase/CompensateAbbreviatedTaxInvoicePdfUseCase` | `handle(KafkaAbbreviatedTaxInvoiceCompensateCommand)` — accepts Kafka DTO, leaks infrastructure |
| `application/service/SagaCommandHandler` | Lives in application layer but handles Kafka routing and DTO extraction — should be in infrastructure |

The `KafkaCommandMapper` is a no-op identity mapper (`toProcess(src)` returns `src`) that provides no value and should be deleted.

## Design

### Target Architecture

```
infrastructure/adapter/in/kafka/dto/
├── ProcessAbbreviatedTaxInvoicePdfCommand    ← extends SagaCommand + Jackson
└── CompensateAbbreviatedTaxInvoicePdfCommand ← extends SagaCommand + Jackson

infrastructure/adapter/in/kafka/
├── SagaCommandHandler                ← (moved from application/service/)
├── SagaRouteConfig                   ← (unchanged)
└── KafkaCommandMapper                ← (DELETE — no-op identity mapper)

application/port/in/
├── ProcessAbbreviatedTaxInvoicePdfUseCase   ← (refactored from usecase/)
└── CompensateAbbreviatedTaxInvoicePdfUseCase ← (refactored from usecase/)

application/service/
└── (empty — SagaCommandHandler moved out)

infrastructure/adapter/out/messaging/
├── SagaReplyPublisher                ← (unchanged, inline AbbreviatedTaxInvoicePdfReplyEvent factory)
└── EventPublisher                    ← (unchanged)
└── AbbreviatedTaxInvoicePdfGeneratedEvent ← (unchanged, stays in out/messaging)

domain/
├── model/   ← (unchanged: AbbreviatedTaxInvoicePdfDocument, GenerationStatus)
├── service/ ← (unchanged: AbbreviatedTaxInvoicePdfGenerationService)
├── repository/ ← (unchanged)
└── exception/ ← (unchanged)
```

### Changes

#### 1. `infrastructure/adapter/in/kafka/dto/`

**Create new DTO files** (replacing `Kafka*` prefixed files):

- `ProcessAbbreviatedTaxInvoicePdfCommand.java` — extends `SagaCommand`, has Jackson annotations (`@JsonProperty`, `@JsonCreator`, `@Getter`) and fields: `documentId`, `documentNumber`, `signedXmlUrl`
- `CompensateAbbreviatedTaxInvoicePdfCommand.java` — extends `SagaCommand`, has Jackson annotations and field: `documentId`

Package: `com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.in.kafka.dto`

#### 2. `application/port/in/`

**Refactor use case interfaces** (move from `application/usecase/` with signature changes):

```java
public interface ProcessAbbreviatedTaxInvoicePdfUseCase {
    void handle(String documentId, String documentNumber, String signedXmlUrl,
                String sagaId, SagaStep sagaStep, String correlationId);
}

public interface CompensateAbbreviatedTaxInvoicePdfUseCase {
    void handle(String documentId, String sagaId, SagaStep sagaStep, String correlationId);
}
```

Package: `com.wpanther.abbreviatedtaxinvoice.pdf.application.port.in`

#### 3. `infrastructure/adapter/in/kafka/SagaCommandHandler`

**Move from** `application/service/SagaCommandHandler.java`
**To** `infrastructure/adapter/in/kafka/SagaCommandHandler.java`

This becomes the **driving adapter** that:
1. Receives Kafka DTOs (`ProcessAbbreviatedTaxInvoicePdfCommand` / `CompensateAbbreviatedTaxInvoicePdfCommand`)
2. Extracts fields and calls use case interfaces with **plain parameters only**
3. Handles all saga orchestration logic (retry tracking, error handling, compensation)

The handler implements both `ProcessAbbreviatedTaxInvoicePdfUseCase` and `CompensateAbbreviatedTaxInvoicePdfUseCase`.

#### 4. `infrastructure/adapter/in/kafka/SagaRouteConfig`

**Modify** to use new DTOs and remove `KafkaCommandMapper`:
- Change `unmarshal().json(JsonLibrary.Jackson, KafkaAbbreviatedTaxInvoiceProcessCommand.class)` → `ProcessAbbreviatedTaxInvoicePdfCommand.class`
- Route processors call `processUseCase.handle(docId, docNumber, signedXmlUrl, sagaId, step, corrId)` directly
- `onPrepareFailure` block uses new DTO class names

#### 5. `infrastructure/adapter/out/messaging/SagaReplyPublisher`

**Unchanged interface**, but inline the `AbbreviatedTaxInvoicePdfReplyEvent` factory methods (the class is referenced from `infrastructure/adapter/out/messaging/`).

#### 6. `infrastructure/adapter/in/kafka/KafkaCommandMapper` — DELETE

No-op identity mapper provides no value. Removed after confirming no other usage.

#### 7. `application/usecase/` — DELETE

After moving interfaces to `application/port/in/`:
- `ProcessAbbreviatedTaxInvoicePdfUseCase.java`
- `CompensateAbbreviatedTaxInvoicePdfUseCase.java`
- `ProcessAbbreviatedTaxInvoicePdfUseCaseImpl.java`
- `CompensateAbbreviatedTaxInvoicePdfUseCaseImpl.java`

#### 8. `application/service/SagaCommandHandler.java` — DELETE (original location)

After moving to `infrastructure/adapter/in/kafka/SagaCommandHandler`.

## Files to Modify

| Action | File |
|--------|------|
| Create | `infrastructure/adapter/in/kafka/dto/ProcessAbbreviatedTaxInvoicePdfCommand.java` |
| Create | `infrastructure/adapter/in/kafka/dto/CompensateAbbreviatedTaxInvoicePdfCommand.java` |
| Create | `application/port/in/ProcessAbbreviatedTaxInvoicePdfUseCase.java` |
| Create | `application/port/in/CompensateAbbreviatedTaxInvoicePdfUseCase.java` |
| Move | `application/service/SagaCommandHandler.java` → `infrastructure/adapter/in/kafka/SagaCommandHandler.java` |
| Delete | `infrastructure/adapter/in/kafka/KafkaAbbreviatedTaxInvoiceProcessCommand.java` |
| Delete | `infrastructure/adapter/in/kafka/KafkaAbbreviatedTaxInvoiceCompensateCommand.java` |
| Delete | `infrastructure/adapter/in/kafka/KafkaCommandMapper.java` |
| Delete | `application/usecase/ProcessAbbreviatedTaxInvoicePdfUseCase.java` |
| Delete | `application/usecase/CompensateAbbreviatedTaxInvoicePdfUseCase.java` |
| Delete | `application/usecase/ProcessAbbreviatedTaxInvoicePdfUseCaseImpl.java` |
| Delete | `application/usecase/CompensateAbbreviatedTaxInvoicePdfUseCaseImpl.java` |
| Delete | `application/service/SagaCommandHandler.java` (original location) |
| Modify | `SagaRouteConfig.java` (use new DTOs, remove KafkaCommandMapper) |
| Modify | `SagaReplyPublisher.java` (confirm inline event factory works) |

## Testing Strategy

1. **Unit tests** — Update all test classes that reference the moved classes:
   - `SagaCommandHandlerTest` → update import for moved handler
   - Any use case tests → update interface references

2. **Kafka consumer tests** — verify `ProcessAbbreviatedTaxInvoicePdfCommand` / `CompensateAbbreviatedTaxInvoicePdfCommand` deserialization still works

3. **Camel route tests** — `SagaRouteConfigTest` needs import updates

4. **Integration test** — Run full service against test infrastructure to verify saga orchestration still functions

## Verification

After refactoring, run:
```bash
mvn clean compile   # Verify no compilation errors
mvn clean test      # Verify all tests pass
```

## Scope

This refactor addresses **only** `abbreviatedtaxinvoice-pdf-generation-service`. Other services with similar patterns should be audited separately.