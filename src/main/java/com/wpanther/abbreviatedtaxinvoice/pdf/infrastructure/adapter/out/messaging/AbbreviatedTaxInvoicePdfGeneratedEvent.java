package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.messaging;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.wpanther.saga.domain.model.TraceEvent;
import lombok.Getter;

import java.util.UUID;

@Getter
public class AbbreviatedTaxInvoicePdfGeneratedEvent extends TraceEvent {

    private static final String EVENT_TYPE = "pdf.generated.abbreviated-tax-invoice";
    private static final String SOURCE = "abbreviatedtaxinvoice-pdf-generation-service";
    private static final String TRACE_TYPE = "PDF_GENERATED";

    @JsonProperty("documentId") private final String documentId;
    @JsonProperty("abbreviatedTaxInvoiceId") private final String abbreviatedTaxInvoiceId;
    @JsonProperty("abbreviatedTaxInvoiceNumber") private final String abbreviatedTaxInvoiceNumber;
    @JsonProperty("documentUrl") private final String documentUrl;
    @JsonProperty("fileSize") private final long fileSize;
    @JsonProperty("xmlEmbedded") private final boolean xmlEmbedded;

    public AbbreviatedTaxInvoicePdfGeneratedEvent(
            @JsonProperty("sagaId") String sagaId,
            @JsonProperty("documentId") String documentId,
            @JsonProperty("abbreviatedTaxInvoiceNumber") String abbreviatedTaxInvoiceNumber,
            @JsonProperty("documentUrl") String documentUrl,
            @JsonProperty("fileSize") long fileSize,
            @JsonProperty("xmlEmbedded") boolean xmlEmbedded,
            @JsonProperty("correlationId") String correlationId) {
        super(sagaId, correlationId, SOURCE, TRACE_TYPE, null);
        this.documentId = documentId;
        this.abbreviatedTaxInvoiceId = sagaId;
        this.abbreviatedTaxInvoiceNumber = abbreviatedTaxInvoiceNumber;
        this.documentUrl = documentUrl;
        this.fileSize = fileSize;
        this.xmlEmbedded = xmlEmbedded;
    }

    @Override
    public String getEventType() {
        return EVENT_TYPE;
    }

    public static AbbreviatedTaxInvoicePdfGeneratedEvent success(
            String documentId, String abbreviatedTaxInvoiceId, String abbreviatedTaxInvoiceNumber,
            String documentUrl, long fileSize) {
        return new AbbreviatedTaxInvoicePdfGeneratedEvent(
                abbreviatedTaxInvoiceId, documentId, abbreviatedTaxInvoiceNumber,
                documentUrl, fileSize, true, abbreviatedTaxInvoiceId);
    }
}
