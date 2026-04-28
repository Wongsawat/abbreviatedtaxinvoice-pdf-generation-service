package com.wpanther.abbreviatedtaxinvoice.pdf.application.port.out;

import com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.messaging.AbbreviatedTaxInvoicePdfGeneratedEvent;

public interface PdfEventPort {
    void publishPdfGenerated(AbbreviatedTaxInvoicePdfGeneratedEvent event);
}
