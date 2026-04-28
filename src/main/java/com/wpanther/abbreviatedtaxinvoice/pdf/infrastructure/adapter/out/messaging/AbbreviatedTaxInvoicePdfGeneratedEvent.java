package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.messaging;

import java.util.UUID;

public class AbbreviatedTaxInvoicePdfGeneratedEvent {
    private String eventId;
    private String eventType;
    private int version;
    private String documentId;
    private String abbreviatedTaxInvoiceId;
    private String abbreviatedTaxInvoiceNumber;
    private String documentUrl;
    private long fileSize;
    private boolean xmlEmbedded;
    private String correlationId;

    public AbbreviatedTaxInvoicePdfGeneratedEvent() {
        this.eventId = UUID.randomUUID().toString();
        this.eventType = "pdf.generated.abbreviated-tax-invoice";
        this.version = 1;
    }

    public AbbreviatedTaxInvoicePdfGeneratedEvent(String sagaId, String documentId, String abbreviatedTaxInvoiceNumber,
                                                   String documentUrl, long fileSize, boolean xmlEmbedded, String correlationId) {
        this();
        this.documentId = documentId;
        this.abbreviatedTaxInvoiceId = sagaId;
        this.abbreviatedTaxInvoiceNumber = abbreviatedTaxInvoiceNumber;
        this.documentUrl = documentUrl;
        this.fileSize = fileSize;
        this.xmlEmbedded = xmlEmbedded;
        this.correlationId = correlationId;
    }

    public String getEventId() { return eventId; }
    public String getEventType() { return eventType; }
    public int getVersion() { return version; }
    public String getDocumentId() { return documentId; }
    public String getAbbreviatedTaxInvoiceId() { return abbreviatedTaxInvoiceId; }
    public String getAbbreviatedTaxInvoiceNumber() { return abbreviatedTaxInvoiceNumber; }
    public String getDocumentUrl() { return documentUrl; }
    public long getFileSize() { return fileSize; }
    public boolean isXmlEmbedded() { return xmlEmbedded; }
    public String getCorrelationId() { return correlationId; }
}
