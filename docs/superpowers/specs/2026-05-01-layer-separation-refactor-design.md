# abbreviatedtaxinvoice-pdf-generation-service Layer Separation Refactoring

**Date:** 2026-05-01
**Author:** Claude Code
**Status:** Draft

## Context

`abbreviatedtaxinvoice-pdf-generation-service` currently has saga infrastructure types (`KafkaAbbreviatedTaxInvoiceProcessCommand`, `KafkaAbbreviatedTaxInvoiceCompensateCommand`) and `KafkaCommandMapper` (no-op) in `infrastructure/adapter/in/kafka/`, but the use case interfaces in `application/usecase/` accept these Kafka command objects directly — violating Hexagonal/Port-Adapters principles. The domain layer should have no framework or infrastructure dependencies.

The reference implementation is `invoice-pdf-generation-service` (refactored 2026-04-30), which correctly separates:
- Kafka DTOs with saga inheritance and Jackson annotations → `infrastructure/adapter/in/kafka/dto/`
- Use case interfaces with plain parameters → `application/port/in/`
- `SagaCommandHandler` in `infrastructure/adapter/in/kafka/` (driving adapter, directly implements port interfaces)
- Reply event factories inlined in `SagaReplyPublisher`

## Problem Statement

Three architectural violations in the current service:

| Location | Violation |
|----------|-----------|
| `application/usecase/ProcessAbbreviatedTaxInvoicePdfUseCase` | `handle(KafkaAbbreviatedTaxInvoiceProcessCommand)` — accepts Kafka DTO, leaks infrastructure |
| `application/usecase/CompensateAbbreviatedTaxInvoicePdfUseCase` | `handle(KafkaAbbreviatedTaxInvoiceCompensateCommand)` — accepts Kafka DTO, leaks infrastructure |
| `application/service/SagaCommandHandler` | Lives in application layer but handles Kafka routing and DTO extraction — should be in infrastructure |

The `KafkaCommandMapper` is a no-op identity mapper (`toProcess(src)` returns `src`) that provides no value and should be deleted.

**Important:** The current `ProcessAbbreviatedTaxInvoicePdfUseCaseImpl` and `CompensateAbbreviatedTaxInvoicePdfUseCaseImpl` are **pure delegation wrappers** — they just forward to `SagaCommandHandler.handle(command)` and `handleCompensation(command)`. After refactoring, these wrapper classes do NOT exist. `SagaCommandHandler` directly implements both port interfaces with plain parameter signatures.

## Design

### Target Architecture

```
infrastructure/adapter/in/kafka/dto/
├── ProcessAbbreviatedTaxInvoicePdfCommand    ← extends SagaCommand + Jackson
└── CompensateAbbreviatedTaxInvoicePdfCommand ← extends SagaCommand + Jackson

infrastructure/adapter/in/kafka/
├── SagaCommandHandler                ← (moved from application/service/, @Service)
├── SagaRouteConfig                   ← (modified to use new DTOs)
└── KafkaCommandMapper                ← (DELETE — no-op identity mapper)

application/port/in/
├── ProcessAbbreviatedTaxInvoicePdfUseCase   ← plain parameters, NO command objects
└── CompensateAbbreviatedTaxInvoicePdfUseCase ← plain parameters, NO command objects

application/service/
└── (empty — SagaCommandHandler moved out, NO *UseCaseImpl classes)

application/usecase/
└── (DELETE — all files removed)

infrastructure/adapter/out/messaging/
├── SagaReplyPublisher                ← (modified, inlines AbbreviatedTaxInvoicePdfReplyEvent factory)
└── EventPublisher                    ← (unchanged)
└── AbbreviatedTaxInvoicePdfGeneratedEvent ← (unchanged, stays here — already correct)

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

**Refactor use case interfaces** (new files, NOT moved from usecase/):

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

Package: `com.wpanther.abbreviatedtaxinvoice.pdf.application.port.in`

#### 3. `infrastructure/adapter/in/kafka/SagaCommandHandler`

**Move from** `application/service/SagaCommandHandler.java`
**To** `infrastructure/adapter/in/kafka/SagaCommandHandler.java`

This is the **driving adapter**. It:
1. Is annotated `@Service`
2. Implements both `ProcessAbbreviatedTaxInvoicePdfUseCase` and `CompensateAbbreviatedTaxInvoicePdfUseCase`
3. Receives Kafka DTOs only in `SagaRouteConfig` (which deserializes and calls the port interface with plain parameters)
4. Contains all saga orchestration logic (retry tracking, error handling, compensation)
5. Has DLQ notification methods called from `SagaRouteConfig.onPrepareFailure`:

```java
// Methods called from SagaRouteConfig onPrepareFailure (plain parameters, no command objects):
public void publishOrchestrationFailure(String sagaId, SagaStep sagaStep, String correlationId, String documentId, String documentNumber, Throwable cause)
public void publishCompensationOrchestrationFailure(String sagaId, SagaStep sagaStep, String correlationId, String documentId, Throwable cause)
public void publishOrchestrationFailureForUnparsedMessage(String sagaId, SagaStep sagaStep, String correlationId, Throwable cause)
```

**Breaking API change:** The current method signatures `handle(KafkaAbbreviatedTaxInvoiceProcessCommand)` and `handleCompensation(KafkaAbbreviatedTaxInvoiceCompensateCommand)` are replaced by:
- `handle(String documentId, String documentNumber, String signedXmlUrl, String sagaId, SagaStep sagaStep, String correlationId)`
- `handle(String documentId, String sagaId, SagaStep sagaStep, String correlationId)`

#### 4. `infrastructure/adapter/in/kafka/SagaRouteConfig`

**Modify** to:
- Use `ProcessAbbreviatedTaxInvoicePdfCommand.class` and `CompensateAbbreviatedTaxInvoicePdfCommand.class` (from `dto/` package)
- Remove `KafkaCommandMapper` field and constructor parameter
- Route processors extract DTO fields and pass **plain parameters** to the port interfaces

Key `.process()` block example (showing field extraction from Kafka DTO):

```java
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
```

`onPrepareFailure` block uses new DTO class names and calls `sagaCommandHandler` methods with plain parameters (no command objects).

#### 5. `infrastructure/adapter/out/messaging/SagaReplyPublisher`

**Modify** to inline `AbbreviatedTaxInvoicePdfReplyEvent` factory as private static inner class. Delete the standalone `AbbreviatedTaxInvoicePdfReplyEvent.java` file after inlining.

#### 6. `infrastructure/adapter/in/kafka/KafkaCommandMapper` — DELETE

No-op identity mapper provides no value. Removed after confirming no other usage.

#### 7. `application/usecase/` — DELETE ENTIRE DIRECTORY

All four files are deleted — there are NO `*UseCaseImpl` classes in the target architecture:
- `ProcessAbbreviatedTaxInvoicePdfUseCase.java`
- `CompensateAbbreviatedTaxInvoicePdfUseCase.java`
- `ProcessAbbreviatedTaxInvoicePdfUseCaseImpl.java`
- `CompensateAbbreviatedTaxInvoicePdfUseCaseImpl.java`

#### 8. `application/service/SagaCommandHandler.java` — DELETE (original location)

After moving to `infrastructure/adapter/in/kafka/SagaCommandHandler`.

#### 9. `AbbreviatedTaxInvoicePdfGeneratedEvent` — STAYS IN PLACE

This event is already correctly placed in `infrastructure/adapter/out/messaging/` (not in `domain/event/`), so no move is needed. This differs from the invoice-pdf-generation-service where `InvoicePdfGeneratedEvent` was incorrectly in `domain/event/` and had to be moved. The abbreviated service already has it in the right location.

#### 10. `AbbreviatedTaxInvoicePdfReplyEvent.java` — DELETE

After inlining its factory into `SagaReplyPublisher`, delete `AbbreviatedTaxInvoicePdfReplyEvent.java`.

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
| Delete | `infrastructure/adapter/out/messaging/AbbreviatedTaxInvoicePdfReplyEvent.java` |
| Modify | `SagaRouteConfig.java` (use new DTOs, remove KafkaCommandMapper, field extraction in .process()) |
| Modify | `SagaReplyPublisher.java` (inline AbbreviatedTaxInvoicePdfReplyEvent factory) |

## Breaking API Changes

Any test or caller that uses the old command-object signatures must be updated:

| Old Signature | New Signature |
|---------------|---------------|
| `SagaCommandHandler.handle(KafkaAbbreviatedTaxInvoiceProcessCommand)` | `SagaCommandHandler.handle(String, String, String, String, SagaStep, String)` |
| `SagaCommandHandler.handleCompensation(KafkaAbbreviatedTaxInvoiceCompensateCommand)` | `SagaCommandHandler.handle(String, String, SagaStep, String)` |
| `processUseCase.handle(command)` | `processUseCase.handle(docId, docNumber, signedXmlUrl, sagaId, step, corrId)` |
| `compensateUseCase.handle(command)` | `compensateUseCase.handle(docId, sagaId, step, corrId)` |

## Testing Strategy

1. **Unit tests** — Any test calling the old command-object signatures will fail to compile and must be updated to use plain parameter signatures
2. **Kafka consumer tests** — verify `ProcessAbbreviatedTaxInvoicePdfCommand` / `CompensateAbbreviatedTaxInvoicePdfCommand` deserialization still works
3. **Camel route tests** — `SagaRouteConfigTest` needs import updates for new DTO package path
4. **Integration test** — Run full service against test infrastructure to verify saga orchestration still functions

## Verification

After refactoring, run:
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
# Expected: SagaCommandHandler.java, SagaRouteConfig.java (NO KafkaCommandMapper)

ls src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/application/usecase/
# Expected: (empty — directory deleted or empty after all files removed)

# 4. Confirm application/service/ is empty or deleted
ls src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/application/service/
# Expected: (empty)
```

## Scope

This refactor addresses **only** `abbreviatedtaxinvoice-pdf-generation-service`. Other services with similar patterns should be audited separately.