package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wpanther.saga.domain.enums.ReplyStatus;
import com.wpanther.saga.domain.enums.SagaStep;
import com.wpanther.saga.domain.model.SagaReply;
import com.wpanther.saga.infrastructure.outbox.OutboxService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Component
public class SagaReplyPublisher implements com.wpanther.abbreviatedtaxinvoice.pdf.application.port.out.SagaReplyPort {

    private static final Logger log = LoggerFactory.getLogger(SagaReplyPublisher.class);

    private final OutboxService outboxService;
    private final ObjectMapper objectMapper;
    private final String replyTopic;

    public SagaReplyPublisher(OutboxService outboxService, ObjectMapper objectMapper,
            @Value("${app.kafka.topics.saga-reply-abbreviated-tax-invoice-pdf:saga.reply.abbreviated-tax-invoice-pdf}") String replyTopic) {
        this.outboxService = outboxService;
        this.objectMapper = objectMapper;
        this.replyTopic = replyTopic;
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void publishSuccess(String sagaId, SagaStep sagaStep, String correlationId, String pdfUrl, long pdfSize) {
        publish(AbbreviatedTaxInvoicePdfReplyEvent.success(sagaId, sagaStep, correlationId, pdfUrl, pdfSize, replyTopic), sagaId);
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void publishFailure(String sagaId, SagaStep sagaStep, String correlationId, String errorMessage) {
        publish(AbbreviatedTaxInvoicePdfReplyEvent.failure(sagaId, sagaStep, correlationId, errorMessage, replyTopic), sagaId);
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void publishCompensated(String sagaId, SagaStep sagaStep, String correlationId) {
        publish(AbbreviatedTaxInvoicePdfReplyEvent.compensated(sagaId, sagaStep, correlationId, replyTopic), sagaId);
    }

    private void publish(AbbreviatedTaxInvoicePdfReplyEvent event, String sagaId) {
        try {
            outboxService.saveWithRouting(
                    event,
                    OutboxConstants.AGGREGATE_TYPE,
                    sagaId,
                    replyTopic,
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
        private final String eventType;

        private AbbreviatedTaxInvoicePdfReplyEvent(
                String sagaId,
                SagaStep sagaStep,
                String correlationId,
                ReplyStatus replyStatus,
                String pdfUrl,
                Long pdfSize,
                String errorMessage,
                String eventType) {
            super(sagaId, sagaStep, correlationId, replyStatus);
            this.pdfUrl = pdfUrl;
            this.pdfSize = pdfSize;
            this.errorMessage = errorMessage;
            this.eventType = eventType;
        }

        public static AbbreviatedTaxInvoicePdfReplyEvent success(
                String sagaId, SagaStep sagaStep, String correlationId, String pdfUrl, Long pdfSize, String eventType) {
            return new AbbreviatedTaxInvoicePdfReplyEvent(
                    sagaId, sagaStep, correlationId, ReplyStatus.SUCCESS, pdfUrl, pdfSize, null, eventType);
        }

        public static AbbreviatedTaxInvoicePdfReplyEvent failure(
                String sagaId, SagaStep sagaStep, String correlationId, String errorMessage, String eventType) {
            return new AbbreviatedTaxInvoicePdfReplyEvent(
                    sagaId, sagaStep, correlationId, ReplyStatus.FAILURE, null, null, errorMessage, eventType);
        }

        public static AbbreviatedTaxInvoicePdfReplyEvent compensated(
                String sagaId, SagaStep sagaStep, String correlationId, String eventType) {
            return new AbbreviatedTaxInvoicePdfReplyEvent(
                    sagaId, sagaStep, correlationId, ReplyStatus.COMPENSATED, null, null, null, eventType);
        }

        @Override
        public String getEventType() {
            return this.eventType;
        }

        public boolean isSuccess() { return ReplyStatus.SUCCESS.equals(getStatus()); }
        public boolean isFailure() { return ReplyStatus.FAILURE.equals(getStatus()); }
        public boolean isCompensated() { return ReplyStatus.COMPENSATED.equals(getStatus()); }
    }
}