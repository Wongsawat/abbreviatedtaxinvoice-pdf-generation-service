package com.wpanther.abbreviatedtaxinvoice.pdf.application.usecase;

import com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.in.kafka.KafkaAbbreviatedTaxInvoiceProcessCommand;

public interface ProcessAbbreviatedTaxInvoicePdfUseCase {
    void handle(KafkaAbbreviatedTaxInvoiceProcessCommand command);
}
