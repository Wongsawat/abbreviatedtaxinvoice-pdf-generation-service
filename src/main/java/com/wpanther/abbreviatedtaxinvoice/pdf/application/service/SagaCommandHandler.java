package com.wpanther.abbreviatedtaxinvoice.pdf.application.service;

import com.wpanther.abbreviatedtaxinvoice.pdf.application.port.out.PdfEventPort;
import com.wpanther.abbreviatedtaxinvoice.pdf.application.port.out.PdfStoragePort;
import com.wpanther.abbreviatedtaxinvoice.pdf.application.port.out.SagaReplyPort;
import com.wpanther.abbreviatedtaxinvoice.pdf.application.port.out.SignedXmlFetchPort;
import com.wpanther.abbreviatedtaxinvoice.pdf.domain.model.AbbreviatedTaxInvoicePdfDocument;
import com.wpanther.abbreviatedtaxinvoice.pdf.domain.model.GenerationStatus;
import com.wpanther.abbreviatedtaxinvoice.pdf.domain.repository.AbbreviatedTaxInvoicePdfDocumentRepository;
import com.wpanther.abbreviatedtaxinvoice.pdf.domain.service.AbbreviatedTaxInvoicePdfGenerationService;
import com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.in.kafka.KafkaAbbreviatedTaxInvoiceCompensateCommand;
import com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.in.kafka.KafkaAbbreviatedTaxInvoiceProcessCommand;
import com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.client.RestTemplateSignedXmlFetcher;
import com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.messaging.AbbreviatedTaxInvoicePdfGeneratedEvent;
import com.wpanther.saga.domain.enums.SagaStep;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
@Slf4j
public class SagaCommandHandler {

    private final AbbreviatedTaxInvoicePdfDocumentRepository documentRepository;
    private final AbbreviatedTaxInvoicePdfGenerationService pdfGenerationService;
    private final SignedXmlFetchPort signedXmlFetchPort;
    private final PdfStoragePort pdfStoragePort;
    private final PdfEventPort pdfEventPort;
    private final SagaReplyPort sagaReplyPort;
    private final Counter successCounter;
    private final Counter failureCounter;

    public SagaCommandHandler(
            AbbreviatedTaxInvoicePdfDocumentRepository documentRepository,
            AbbreviatedTaxInvoicePdfGenerationService pdfGenerationService,
            SignedXmlFetchPort signedXmlFetchPort,
            PdfStoragePort pdfStoragePort,
            PdfEventPort pdfEventPort,
            SagaReplyPort sagaReplyPort,
            MeterRegistry meterRegistry) {
        this.documentRepository = documentRepository;
        this.pdfGenerationService = pdfGenerationService;
        this.signedXmlFetchPort = signedXmlFetchPort;
        this.pdfStoragePort = pdfStoragePort;
        this.pdfEventPort = pdfEventPort;
        this.sagaReplyPort = sagaReplyPort;
        this.successCounter = meterRegistry.counter("pdf.generation.success");
        this.failureCounter = meterRegistry.counter("pdf.generation.failure");
    }

    public void handle(KafkaAbbreviatedTaxInvoiceProcessCommand command) {
        log.info("Handling saga command for saga: {}, document: {}",
                command.getSagaId(), command.getDocumentNumber());

        Optional<AbbreviatedTaxInvoicePdfDocument> existingDoc =
                documentRepository.findByAbbreviatedTaxInvoiceId(command.getDocumentId());

        AbbreviatedTaxInvoicePdfDocument document = existingDoc.orElseGet(() ->
                new AbbreviatedTaxInvoicePdfDocument(command.getDocumentId(), command.getDocumentNumber()));

        try {
            document.startGeneration();
            documentRepository.save(document);

            String signedXml = signedXmlFetchPort.fetch(command.getSignedXmlUrl());
            log.debug("Fetched signed XML: {} bytes", signedXml.length());

            byte[] pdfBytes = pdfGenerationService.generatePdf(
                    document.getAbbreviatedTaxInvoiceNumber(), signedXml);

            String s3Key = pdfStoragePort.store(document.getAbbreviatedTaxInvoiceNumber(), pdfBytes);
            String pdfUrl = pdfStoragePort.resolveUrl(s3Key);

            document.markCompleted(s3Key, pdfUrl, (long) pdfBytes.length);
            documentRepository.save(document);

            pdfEventPort.publishPdfGenerated(AbbreviatedTaxInvoicePdfGeneratedEvent.success(
                    document.getId().toString(),
                    document.getAbbreviatedTaxInvoiceId(),
                    document.getAbbreviatedTaxInvoiceNumber(),
                    pdfUrl,
                    (long) pdfBytes.length
            ));

            sagaReplyPort.publishSuccess(
                    command.getSagaId(),
                    SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF,
                    command.getCorrelationId(),
                    pdfUrl,
                    (long) pdfBytes.length
            );

            successCounter.increment();
            log.info("PDF generation completed for document: {}", document.getAbbreviatedTaxInvoiceNumber());

        } catch (RestTemplateSignedXmlFetcher.SignedXmlFetchException e) {
            log.error("Failed to fetch signed XML for document: {}", command.getDocumentNumber(), e);
            handleFailure(document, command, e, "Failed to fetch signed XML: " + e.getMessage());
        } catch (AbbreviatedTaxInvoicePdfGenerationService.AbbreviatedTaxInvoicePdfGenerationException e) {
            log.error("PDF generation failed for document: {}", command.getDocumentNumber(), e);
            handleFailure(document, command, e, "PDF generation failed: " + e.getMessage());
        } catch (Exception e) {
            log.error("Unexpected error during PDF generation for document: {}", command.getDocumentNumber(), e);
            handleFailure(document, command, e, "Unexpected error: " + e.getMessage());
        }
    }

    private void handleFailure(AbbreviatedTaxInvoicePdfDocument document,
                               KafkaAbbreviatedTaxInvoiceProcessCommand command,
                               Throwable cause,
                               String errorMessage) {
        try {
            document.incrementRetry();
            if (document.hasExceededMaxRetries()) {
                document.markFailed(errorMessage);
                log.error("PDF generation exhausted retries for document: {}, failing permanently",
                        document.getAbbreviatedTaxInvoiceNumber());
            } else {
                log.warn("PDF generation failed for document: {}, will retry (attempt {})",
                        document.getAbbreviatedTaxInvoiceNumber(), document.getRetryCount());
            }
            documentRepository.save(document);

            sagaReplyPort.publishFailure(
                    command.getSagaId(),
                    SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF,
                    command.getCorrelationId(),
                    errorMessage
            );

            failureCounter.increment();
        } catch (Exception e) {
            log.error("Failed to handle failure for document: {}", document.getAbbreviatedTaxInvoiceId(), e);
        }
    }

    public void handleCompensation(KafkaAbbreviatedTaxInvoiceCompensateCommand command) {
        log.info("Handling compensation for saga: {}, document: {}",
                command.getSagaId(), command.getDocumentId());

        try {
            Optional<AbbreviatedTaxInvoicePdfDocument> docOpt =
                    documentRepository.findByAbbreviatedTaxInvoiceId(command.getDocumentId());

            if (docOpt.isPresent()) {
                AbbreviatedTaxInvoicePdfDocument document = docOpt.get();
                if (document.getDocumentPath() != null) {
                    pdfStoragePort.delete(document.getDocumentPath());
                    log.info("Deleted PDF from storage: {}", document.getDocumentPath());
                }
                document.markCompensated();
                documentRepository.save(document);
            }

            sagaReplyPort.publishCompensated(
                    command.getSagaId(),
                    SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF,
                    command.getCorrelationId()
            );

            log.info("Compensation completed for document: {}", command.getDocumentId());

        } catch (Exception e) {
            log.error("Compensation failed for document: {}", command.getDocumentId(), e);
            sagaReplyPort.publishFailure(
                    command.getSagaId(),
                    SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF,
                    command.getCorrelationId(),
                    "Compensation failed: " + e.getMessage()
            );
        }
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