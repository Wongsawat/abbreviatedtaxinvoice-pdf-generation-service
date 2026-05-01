package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
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
public class SagaReplyPublisher implements com.wpanther.abbreviatedtaxinvoice.pdf.application.port.out.SagaReplyPort {

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
            super(sagaId, sagaStep, correlationId, ReplyStatus.FAILURE);
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