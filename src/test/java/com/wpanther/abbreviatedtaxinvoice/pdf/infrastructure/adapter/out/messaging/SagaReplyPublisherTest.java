package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.messaging;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wpanther.saga.domain.enums.ReplyStatus;
import com.wpanther.saga.domain.enums.SagaStep;
import com.wpanther.saga.infrastructure.outbox.OutboxService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
@DisplayName("SagaReplyPublisher Tests")
class SagaReplyPublisherTest {

    @Mock
    private OutboxService outboxService;

    private SagaReplyPublisher publisher;

    @BeforeEach
    void setUp() {
        publisher = new SagaReplyPublisher(outboxService, new ObjectMapper());
    }

    @Test
    @DisplayName("publishSuccess() sends success reply with PDF info")
    void testPublishSuccess() {
        publisher.publishSuccess("saga-123", SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF,
                "corr-456", "http://example.com/doc.pdf", 1024L);

        ArgumentCaptor<com.wpanther.saga.domain.model.IntegrationEvent> eventCaptor = ArgumentCaptor.forClass(com.wpanther.saga.domain.model.IntegrationEvent.class);
        verify(outboxService).saveWithRouting(
                eventCaptor.capture(),
                eq("AbbreviatedTaxInvoicePdfDocument"),
                eq("saga-123"),
                eq("saga.reply.abbreviated-tax-invoice-pdf"),
                eq("saga-123"),
                any()
        );

        AbbreviatedTaxInvoicePdfReplyEvent replyEvent = (AbbreviatedTaxInvoicePdfReplyEvent) eventCaptor.getValue();
        assertThat(replyEvent.isSuccess()).isTrue();
        assertThat(replyEvent.getPdfUrl()).isEqualTo("http://example.com/doc.pdf");
        assertThat(replyEvent.getPdfSize()).isEqualTo(1024L);
    }

    @Test
    @DisplayName("publishFailure() sends failure reply with error message")
    void testPublishFailure() {
        publisher.publishFailure("saga-123", SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF,
                "corr-456", "Something went wrong");

        ArgumentCaptor<com.wpanther.saga.domain.model.IntegrationEvent> eventCaptor = ArgumentCaptor.forClass(com.wpanther.saga.domain.model.IntegrationEvent.class);
        verify(outboxService).saveWithRouting(
                eventCaptor.capture(),
                eq("AbbreviatedTaxInvoicePdfDocument"),
                eq("saga-123"),
                eq("saga.reply.abbreviated-tax-invoice-pdf"),
                eq("saga-123"),
                any()
        );

        AbbreviatedTaxInvoicePdfReplyEvent failureEvent = (AbbreviatedTaxInvoicePdfReplyEvent) eventCaptor.getValue();
        assertThat(failureEvent.isFailure()).isTrue();
        assertThat(failureEvent.getErrorMessage()).isEqualTo("Something went wrong");
    }

    @Test
    @DisplayName("publishCompensated() sends compensated reply")
    void testPublishCompensated() {
        publisher.publishCompensated("saga-123", SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF, "corr-456");

        ArgumentCaptor<com.wpanther.saga.domain.model.IntegrationEvent> eventCaptor = ArgumentCaptor.forClass(com.wpanther.saga.domain.model.IntegrationEvent.class);
        verify(outboxService).saveWithRouting(
                eventCaptor.capture(),
                eq("AbbreviatedTaxInvoicePdfDocument"),
                eq("saga-123"),
                eq("saga.reply.abbreviated-tax-invoice-pdf"),
                eq("saga-123"),
                any()
        );

        AbbreviatedTaxInvoicePdfReplyEvent compensatedEvent = (AbbreviatedTaxInvoicePdfReplyEvent) eventCaptor.getValue();
        assertThat(compensatedEvent.isCompensated()).isTrue();
    }
}