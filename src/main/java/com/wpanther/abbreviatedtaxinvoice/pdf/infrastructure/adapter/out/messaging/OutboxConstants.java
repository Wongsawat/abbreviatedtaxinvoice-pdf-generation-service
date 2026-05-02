package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.messaging;

public final class OutboxConstants {
    private OutboxConstants() {}

    public static final String TOPIC_PDF_GENERATED = "pdf.generated.abbreviated-tax-invoice";
    public static final String AGGREGATE_TYPE = "AbbreviatedTaxInvoicePdfDocument";
}