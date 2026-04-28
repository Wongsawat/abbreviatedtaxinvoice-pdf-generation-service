package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.in.kafka;

import com.wpanther.saga.domain.enums.SagaStep;
import java.util.UUID;

public class KafkaAbbreviatedTaxInvoiceProcessCommand {
    private String eventId;
    private SagaStep sagaStep;
    private String correlationId;
    private String documentId;
    private String documentNumber;
    private String signedXmlUrl;

    public KafkaAbbreviatedTaxInvoiceProcessCommand() {}

    public KafkaAbbreviatedTaxInvoiceProcessCommand(String sagaId, SagaStep sagaStep, String correlationId,
                                                     String documentId, String documentNumber, String signedXmlUrl) {
        this.eventId = UUID.randomUUID().toString();
        this.sagaStep = sagaStep;
        this.correlationId = correlationId;
        this.documentId = documentId;
        this.documentNumber = documentNumber;
        this.signedXmlUrl = signedXmlUrl;
    }

    public String getEventId() { return eventId; }
    public SagaStep getSagaStep() { return sagaStep; }
    public String getCorrelationId() { return correlationId; }
    public String getDocumentId() { return documentId; }
    public String getDocumentNumber() { return documentNumber; }
    public String getSignedXmlUrl() { return signedXmlUrl; }
}
