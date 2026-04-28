package com.wpanther.abbreviatedtaxinvoice.pdf.application.usecase;

import com.wpanther.abbreviatedtaxinvoice.pdf.application.service.SagaCommandHandler;
import com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.in.kafka.KafkaAbbreviatedTaxInvoiceCompensateCommand;
import org.springframework.stereotype.Component;

@Component
public class CompensateAbbreviatedTaxInvoicePdfUseCaseImpl implements CompensateAbbreviatedTaxInvoicePdfUseCase {

    private final SagaCommandHandler sagaCommandHandler;

    public CompensateAbbreviatedTaxInvoicePdfUseCaseImpl(SagaCommandHandler sagaCommandHandler) {
        this.sagaCommandHandler = sagaCommandHandler;
    }

    @Override
    public void handle(KafkaAbbreviatedTaxInvoiceCompensateCommand command) {
        sagaCommandHandler.handleCompensation(command);
    }
}
