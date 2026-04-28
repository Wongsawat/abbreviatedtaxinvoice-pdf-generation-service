package com.wpanther.abbreviatedtaxinvoice.pdf.application.usecase;

import com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.in.kafka.KafkaAbbreviatedTaxInvoiceCompensateCommand;

public interface CompensateAbbreviatedTaxInvoicePdfUseCase {
    void handle(KafkaAbbreviatedTaxInvoiceCompensateCommand command);
}
