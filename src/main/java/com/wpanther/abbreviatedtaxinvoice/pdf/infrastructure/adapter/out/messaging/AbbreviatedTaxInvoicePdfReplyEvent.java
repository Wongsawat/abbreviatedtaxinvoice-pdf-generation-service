package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.messaging;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.wpanther.saga.domain.enums.ReplyStatus;
import com.wpanther.saga.domain.enums.SagaStep;
import com.wpanther.saga.domain.model.SagaReply;
import lombok.Getter;

@Getter
public class AbbreviatedTaxInvoicePdfReplyEvent extends SagaReply {

    private final String pdfUrl;
    private final Long pdfSize;
    private final String errorMessage;

    public AbbreviatedTaxInvoicePdfReplyEvent(
            @JsonProperty("sagaId") String sagaId,
            @JsonProperty("sagaStep") SagaStep sagaStep,
            @JsonProperty("correlationId") String correlationId,
            @JsonProperty("replyStatus") ReplyStatus replyStatus,
            @JsonProperty("pdfUrl") String pdfUrl,
            @JsonProperty("pdfSize") Long pdfSize,
            @JsonProperty("errorMessage") String errorMessage) {
        super(sagaId, sagaStep, correlationId, replyStatus);
        this.pdfUrl = pdfUrl;
        this.pdfSize = pdfSize;
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
                sagaId, sagaStep, correlationId, ReplyStatus.FAILURE, null, null, errorMessage);
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
