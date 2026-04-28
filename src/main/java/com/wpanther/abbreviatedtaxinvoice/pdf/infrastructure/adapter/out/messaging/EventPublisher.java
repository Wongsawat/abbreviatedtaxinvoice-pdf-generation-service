package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wpanther.saga.infrastructure.outbox.OutboxService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventPublisher {
    private static final Logger log = LoggerFactory.getLogger(EventPublisher.class);
    private final OutboxService outboxService;
    private final ObjectMapper objectMapper;

    public EventPublisher(OutboxService outboxService, ObjectMapper objectMapper) {
        this.outboxService = outboxService;
        this.objectMapper = objectMapper;
    }

    public void publishPdfGenerated(AbbreviatedTaxInvoicePdfGeneratedEvent event) {
        try {
            String headers = String.format(
                    "{\"documentType\":\"ABBREVIATED_TAX_INVOICE\",\"correlationId\":\"%s\"}",
                    event.getCorrelationId());
            outboxService.saveWithRouting(
                    event,
                    OutboxConstants.AGGREGATE_TYPE,
                    event.getDocumentId(),
                    OutboxConstants.TOPIC_PDF_GENERATED,
                    event.getDocumentId(),
                    headers
            );
            log.info("Published pdf.generated.abbreviated-tax-invoice event for document: {}", event.getDocumentId());
        } catch (Exception e) {
            throw new RuntimeException("Failed to publish AbbreviatedTaxInvoicePdfGeneratedEvent", e);
        }
    }
}
