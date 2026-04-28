package com.wpanther.abbreviatedtaxinvoice.pdf.application.usecase;

import com.wpanther.abbreviatedtaxinvoice.pdf.application.service.SagaCommandHandler;
import com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.in.kafka.KafkaAbbreviatedTaxInvoiceProcessCommand;
import org.springframework.stereotype.Component;

@Component
public class ProcessAbbreviatedTaxInvoicePdfUseCaseImpl implements ProcessAbbreviatedTaxInvoicePdfUseCase {

    private final SagaCommandHandler sagaCommandHandler;

    public ProcessAbbreviatedTaxInvoicePdfUseCaseImpl(SagaCommandHandler sagaCommandHandler) {
        this.sagaCommandHandler = sagaCommandHandler;
    }

    @Override
    public void handle(KafkaAbbreviatedTaxInvoiceProcessCommand command) {
        sagaCommandHandler.handle(command);
    }
}
