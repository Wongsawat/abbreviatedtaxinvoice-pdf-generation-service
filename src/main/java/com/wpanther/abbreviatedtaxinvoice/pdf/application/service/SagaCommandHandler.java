package com.wpanther.abbreviatedtaxinvoice.pdf.application.service;

import com.wpanther.abbreviatedtaxinvoice.pdf.application.port.out.PdfEventPort;
import com.wpanther.abbreviatedtaxinvoice.pdf.application.port.out.PdfStoragePort;
import com.wpanther.abbreviatedtaxinvoice.pdf.application.port.out.SagaReplyPort;
import com.wpanther.abbreviatedtaxinvoice.pdf.application.port.out.SignedXmlFetchPort;
import com.wpanther.abbreviatedtaxinvoice.pdf.domain.model.AbbreviatedTaxInvoicePdfDocument;
import com.wpanther.abbreviatedtaxinvoice.pdf.domain.repository.AbbreviatedTaxInvoicePdfDocumentRepository;
import com.wpanther.abbreviatedtaxinvoice.pdf.domain.service.AbbreviatedTaxInvoicePdfGenerationService;
import com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.in.kafka.KafkaAbbreviatedTaxInvoiceCompensateCommand;
import com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.in.kafka.KafkaAbbreviatedTaxInvoiceProcessCommand;
import com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.messaging.AbbreviatedTaxInvoicePdfGeneratedEvent;
import com.wpanther.saga.domain.enums.SagaStep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class SagaCommandHandler {

    private static final Logger log = LoggerFactory.getLogger(SagaCommandHandler.class);

    private final AbbreviatedTaxInvoicePdfDocumentRepository documentRepository;
    private final AbbreviatedTaxInvoicePdfGenerationService pdfGenerationService;
    private final SignedXmlFetchPort signedXmlFetchPort;
    private final PdfStoragePort pdfStoragePort;
    private final PdfEventPort pdfEventPort;
    private final SagaReplyPort sagaReplyPort;

    public SagaCommandHandler(
            AbbreviatedTaxInvoicePdfDocumentRepository documentRepository,
            AbbreviatedTaxInvoicePdfGenerationService pdfGenerationService,
            SignedXmlFetchPort signedXmlFetchPort,
            PdfStoragePort pdfStoragePort,
            PdfEventPort pdfEventPort,
            SagaReplyPort sagaReplyPort) {
        this.documentRepository = documentRepository;
        this.pdfGenerationService = pdfGenerationService;
        this.signedXmlFetchPort = signedXmlFetchPort;
        this.pdfStoragePort = pdfStoragePort;
        this.pdfEventPort = pdfEventPort;
        this.sagaReplyPort = sagaReplyPort;
    }

    public void handle(KafkaAbbreviatedTaxInvoiceProcessCommand command) {
        log.info("Handling saga command for saga: {}, document: {}", command.getSagaId(), command.getDocumentNumber());
    }

    public void handleCompensation(KafkaAbbreviatedTaxInvoiceCompensateCommand command) {
        log.info("Handling compensation for saga: {}, document: {}", command.getSagaId(), command.getDocumentId());
    }

    public void publishOrchestrationFailure(KafkaAbbreviatedTaxInvoiceProcessCommand command, Throwable cause) {
        sagaReplyPort.publishFailure(command.getSagaId(), SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF,
                command.getCorrelationId(), cause.getMessage());
    }

    public void publishCompensationOrchestrationFailure(KafkaAbbreviatedTaxInvoiceCompensateCommand command, Throwable cause) {
        sagaReplyPort.publishFailure(command.getSagaId(), SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF,
                command.getCorrelationId(), cause.getMessage());
    }

    public void publishOrchestrationFailureForUnparsedMessage(String sagaId, SagaStep sagaStep, String correlationId, Throwable cause) {
        sagaReplyPort.publishFailure(sagaId, sagaStep, correlationId, cause.getMessage());
    }
}
