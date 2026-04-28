package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wpanther.saga.domain.enums.SagaStep;
import com.wpanther.saga.infrastructure.outbox.OutboxService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SagaReplyPublisher {
    private static final Logger log = LoggerFactory.getLogger(SagaReplyPublisher.class);
    private final OutboxService outboxService;
    private final ObjectMapper objectMapper;

    public SagaReplyPublisher(OutboxService outboxService, ObjectMapper objectMapper) {
        this.outboxService = outboxService;
        this.objectMapper = objectMapper;
    }

    public void publishSuccess(String sagaId, SagaStep sagaStep, String correlationId, String pdfUrl, long pdfSize) {
        publish(AbbreviatedTaxInvoicePdfReplyEvent.success(sagaId, sagaStep, correlationId, pdfUrl, pdfSize), sagaId);
    }

    public void publishFailure(String sagaId, SagaStep sagaStep, String correlationId, String errorMessage) {
        publish(AbbreviatedTaxInvoicePdfReplyEvent.failure(sagaId, sagaStep, correlationId, errorMessage), sagaId);
    }

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
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize AbbreviatedTaxInvoicePdfReplyEvent", e);
        }
    }
}
