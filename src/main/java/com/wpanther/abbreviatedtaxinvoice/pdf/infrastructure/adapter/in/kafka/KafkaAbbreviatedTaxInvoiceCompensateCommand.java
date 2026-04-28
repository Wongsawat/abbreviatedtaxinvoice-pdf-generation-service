package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.in.kafka;

import com.wpanther.saga.domain.enums.SagaStep;
import java.util.UUID;

public class KafkaAbbreviatedTaxInvoiceCompensateCommand {
    private String eventId;
    private SagaStep sagaStep;
    private String correlationId;
    private String documentId;

    public KafkaAbbreviatedTaxInvoiceCompensateCommand() {}

    public KafkaAbbreviatedTaxInvoiceCompensateCommand(String sagaId, SagaStep sagaStep, String correlationId, String documentId) {
        this.eventId = UUID.randomUUID().toString();
        this.sagaStep = sagaStep;
        this.correlationId = correlationId;
        this.documentId = documentId;
    }

    public String getEventId() { return eventId; }
    public SagaStep getSagaStep() { return sagaStep; }
    public String getCorrelationId() { return correlationId; }
    public String getDocumentId() { return documentId; }
}
