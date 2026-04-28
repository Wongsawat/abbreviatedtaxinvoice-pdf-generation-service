package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.messaging;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wpanther.saga.infrastructure.outbox.OutboxService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
@DisplayName("EventPublisher Tests")
class EventPublisherTest {

    @Mock
    private OutboxService outboxService;

    private EventPublisher publisher;

    @BeforeEach
    void setUp() {
        publisher = new EventPublisher(outboxService, new ObjectMapper());
    }

    @Test
    @DisplayName("publishPdfGenerated() saves event with correct routing")
    void testPublishPdfGenerated() {
        AbbreviatedTaxInvoicePdfGeneratedEvent event = AbbreviatedTaxInvoicePdfGeneratedEvent.success(
                "doc-123", "ATINV-001", "ATINV-001", "http://example.com/doc.pdf", 1024);

        publisher.publishPdfGenerated(event);

        ArgumentCaptor<com.wpanther.saga.domain.model.IntegrationEvent> eventCaptor = ArgumentCaptor.forClass(com.wpanther.saga.domain.model.IntegrationEvent.class);
        verify(outboxService).saveWithRouting(
                eventCaptor.capture(),
                eq("AbbreviatedTaxInvoicePdfDocument"),
                eq("doc-123"),
                eq("pdf.generated.abbreviated-tax-invoice"),
                eq("doc-123"),
                any()
        );
        AbbreviatedTaxInvoicePdfGeneratedEvent capturedEvent = (AbbreviatedTaxInvoicePdfGeneratedEvent) eventCaptor.getValue();
        assertThat(capturedEvent).isSameAs(event);
    }
}