package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class PdfGenerationMetrics {

    private static final String RETRY_EXHAUSTED_COUNTER = "pdf.generation.retry.exhausted";
    private final MeterRegistry meterRegistry;

    public PdfGenerationMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        Counter.builder(RETRY_EXHAUSTED_COUNTER)
                .description("Number of times PDF generation max retries were exceeded")
                .register(meterRegistry);
    }

    public void recordRetryExhausted(String sagaId, String abbreviatedTaxInvoiceId,
                                     String abbreviatedTaxInvoiceNumber) {
        meterRegistry.counter(RETRY_EXHAUSTED_COUNTER,
                "saga_id", sagaId,
                "abbreviated_tax_invoice_id", abbreviatedTaxInvoiceId,
                "abbreviated_tax_invoice_number", abbreviatedTaxInvoiceNumber)
                .increment();
    }
}
