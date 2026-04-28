package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.messaging;

import com.wpanther.saga.domain.enums.SagaStep;
import java.util.UUID;

public class AbbreviatedTaxInvoicePdfReplyEvent {
    private String eventId;
    private String sagaId;
    private SagaStep sagaStep;
    private String correlationId;
    private String status;
    private String pdfUrl;
    private Long pdfSize;
    private String errorMessage;

    public AbbreviatedTaxInvoicePdfReplyEvent() {
        this.eventId = UUID.randomUUID().toString();
    }

    public static AbbreviatedTaxInvoicePdfReplyEvent success(String sagaId, SagaStep sagaStep, String correlationId, String pdfUrl, Long pdfSize) {
        AbbreviatedTaxInvoicePdfReplyEvent e = new AbbreviatedTaxInvoicePdfReplyEvent();
        e.sagaId = sagaId;
        e.sagaStep = sagaStep;
        e.correlationId = correlationId;
        e.status = "SUCCESS";
        e.pdfUrl = pdfUrl;
        e.pdfSize = pdfSize;
        return e;
    }

    public static AbbreviatedTaxInvoicePdfReplyEvent failure(String sagaId, SagaStep sagaStep, String correlationId, String errorMessage) {
        AbbreviatedTaxInvoicePdfReplyEvent e = new AbbreviatedTaxInvoicePdfReplyEvent();
        e.sagaId = sagaId;
        e.sagaStep = sagaStep;
        e.correlationId = correlationId;
        e.status = "FAILURE";
        e.errorMessage = errorMessage;
        return e;
    }

    public static AbbreviatedTaxInvoicePdfReplyEvent compensated(String sagaId, SagaStep sagaStep, String correlationId) {
        AbbreviatedTaxInvoicePdfReplyEvent e = new AbbreviatedTaxInvoicePdfReplyEvent();
        e.sagaId = sagaId;
        e.sagaStep = sagaStep;
        e.correlationId = correlationId;
        e.status = "COMPENSATED";
        return e;
    }

    public boolean isSuccess() { return "SUCCESS".equals(status); }
    public boolean isFailure() { return "FAILURE".equals(status); }
    public boolean isCompensated() { return "COMPENSATED".equals(status); }

    public String getEventId() { return eventId; }
    public String getSagaId() { return sagaId; }
    public SagaStep getSagaStep() { return sagaStep; }
    public String getCorrelationId() { return correlationId; }
    public String getStatus() { return status; }
    public String getPdfUrl() { return pdfUrl; }
    public Long getPdfSize() { return pdfSize; }
    public String getErrorMessage() { return errorMessage; }
}
