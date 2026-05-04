package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.in.kafka;

import com.wpanther.abbreviatedtaxinvoice.pdf.application.dto.event.DocumentArchiveEvent;
import com.wpanther.abbreviatedtaxinvoice.pdf.application.port.in.CompensateAbbreviatedTaxInvoicePdfUseCase;
import com.wpanther.abbreviatedtaxinvoice.pdf.application.port.in.ProcessAbbreviatedTaxInvoicePdfUseCase;
import com.wpanther.abbreviatedtaxinvoice.pdf.application.port.out.DocumentArchivePort;
import com.wpanther.abbreviatedtaxinvoice.pdf.application.port.out.PdfEventPort;
import com.wpanther.abbreviatedtaxinvoice.pdf.application.port.out.PdfStoragePort;
import com.wpanther.abbreviatedtaxinvoice.pdf.application.port.out.SagaReplyPort;
import com.wpanther.abbreviatedtaxinvoice.pdf.application.port.out.SignedXmlFetchPort;
import com.wpanther.abbreviatedtaxinvoice.pdf.domain.model.AbbreviatedTaxInvoicePdfDocument;
import com.wpanther.abbreviatedtaxinvoice.pdf.domain.repository.AbbreviatedTaxInvoicePdfDocumentRepository;
import com.wpanther.abbreviatedtaxinvoice.pdf.domain.service.AbbreviatedTaxInvoicePdfGenerationService;
import com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.client.RestTemplateSignedXmlFetcher;
import com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.messaging.AbbreviatedTaxInvoicePdfGeneratedEvent;
import com.wpanther.saga.domain.enums.SagaStep;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@Slf4j
public class SagaCommandHandler implements ProcessAbbreviatedTaxInvoicePdfUseCase, CompensateAbbreviatedTaxInvoicePdfUseCase {

    private final AbbreviatedTaxInvoicePdfDocumentRepository documentRepository;
    private final AbbreviatedTaxInvoicePdfGenerationService pdfGenerationService;
    private final SignedXmlFetchPort signedXmlFetchPort;
    private final PdfStoragePort pdfStoragePort;
    private final PdfEventPort pdfEventPort;
    private final DocumentArchivePort documentArchivePort;
    private final SagaReplyPort sagaReplyPort;
    private final Counter successCounter;
    private final Counter failureCounter;

    public SagaCommandHandler(
            AbbreviatedTaxInvoicePdfDocumentRepository documentRepository,
            AbbreviatedTaxInvoicePdfGenerationService pdfGenerationService,
            SignedXmlFetchPort signedXmlFetchPort,
            PdfStoragePort pdfStoragePort,
            PdfEventPort pdfEventPort,
            DocumentArchivePort documentArchivePort,
            SagaReplyPort sagaReplyPort,
            MeterRegistry meterRegistry) {
        this.documentRepository = documentRepository;
        this.pdfGenerationService = pdfGenerationService;
        this.signedXmlFetchPort = signedXmlFetchPort;
        this.pdfStoragePort = pdfStoragePort;
        this.pdfEventPort = pdfEventPort;
        this.documentArchivePort = documentArchivePort;
        this.sagaReplyPort = sagaReplyPort;
        this.successCounter = meterRegistry.counter("pdf.generation.success");
        this.failureCounter = meterRegistry.counter("pdf.generation.failure");
    }

    @Override
    public void handle(String documentId, String documentNumber, String signedXmlUrl,
                       String sagaId, SagaStep sagaStep, String correlationId) {
        log.info("Handling saga command for saga: {}, document: {}", sagaId, documentNumber);

        Optional<AbbreviatedTaxInvoicePdfDocument> existingDoc =
                documentRepository.findByAbbreviatedTaxInvoiceId(documentId);

        AbbreviatedTaxInvoicePdfDocument document = existingDoc.orElseGet(() ->
                new AbbreviatedTaxInvoicePdfDocument(documentId, documentNumber));

        try {
            document.startGeneration();
            documentRepository.save(document);

            String xml = signedXmlFetchPort.fetch(signedXmlUrl);
            log.debug("Fetched signed XML: {} bytes", xml.length());

            byte[] pdfBytes = pdfGenerationService.generatePdf(
                    document.getAbbreviatedTaxInvoiceNumber(), xml);

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

            // Emit document.archive for unsigned PDF archival
            documentArchivePort.publish(new DocumentArchiveEvent(
                    document.getAbbreviatedTaxInvoiceId(),
                    document.getAbbreviatedTaxInvoiceNumber(),
                    "ABBREVIATED_TAX_INVOICE",
                    "UNSIGNED_PDF",
                    pdfUrl,
                    document.getAbbreviatedTaxInvoiceNumber() + ".pdf",
                    "application/pdf",
                    (long) pdfBytes.length,
                    sagaId,
                    correlationId
            ));

            sagaReplyPort.publishSuccess(
                    sagaId,
                    SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF,
                    correlationId,
                    pdfUrl,
                    (long) pdfBytes.length
            );

            successCounter.increment();
            log.info("PDF generation completed for document: {}", document.getAbbreviatedTaxInvoiceNumber());

        } catch (RestTemplateSignedXmlFetcher.SignedXmlFetchException e) {
            log.error("Failed to fetch signed XML for document: {}", documentNumber, e);
            handleFailure(document, sagaId, correlationId, e, "Failed to fetch signed XML: " + e.getMessage());
        } catch (AbbreviatedTaxInvoicePdfGenerationService.AbbreviatedTaxInvoicePdfGenerationException e) {
            log.error("PDF generation failed for document: {}", documentNumber, e);
            handleFailure(document, sagaId, correlationId, e, "PDF generation failed: " + e.getMessage());
        } catch (Exception e) {
            log.error("Unexpected error during PDF generation for document: {}", documentNumber, e);
            handleFailure(document, sagaId, correlationId, e, "Unexpected error: " + e.getMessage());
        }
    }

    private void handleFailure(AbbreviatedTaxInvoicePdfDocument document,
                               String sagaId, String correlationId,
                               Throwable cause, String errorMessage) {
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
                    sagaId,
                    SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF,
                    correlationId,
                    errorMessage
            );

            failureCounter.increment();
        } catch (Exception e) {
            log.error("Failed to handle failure for document: {}", document.getAbbreviatedTaxInvoiceId(), e);
        }
    }

    @Override
    public void handle(String documentId, String sagaId, SagaStep sagaStep, String correlationId) {
        log.info("Handling compensation for saga: {}, document: {}", sagaId, documentId);

        try {
            Optional<AbbreviatedTaxInvoicePdfDocument> docOpt =
                    documentRepository.findByAbbreviatedTaxInvoiceId(documentId);

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
                    sagaId,
                    SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF,
                    correlationId
            );

            log.info("Compensation completed for document: {}", documentId);

        } catch (Exception e) {
            log.error("Compensation failed for document: {}", documentId, e);
            sagaReplyPort.publishFailure(
                    sagaId,
                    SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF,
                    correlationId,
                    "Compensation failed: " + e.getMessage()
            );
        }
    }

    public void publishOrchestrationFailure(String sagaId, String correlationId, String documentId, String documentNumber, Throwable cause) {
        sagaReplyPort.publishFailure(
                sagaId,
                SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF,
                correlationId,
                cause.getMessage()
        );
    }

    public void publishCompensationOrchestrationFailure(String sagaId, String correlationId, String documentId, Throwable cause) {
        sagaReplyPort.publishFailure(
                sagaId,
                SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF,
                correlationId,
                cause.getMessage()
        );
    }

    public void publishOrchestrationFailureForUnparsedMessage(String sagaId, SagaStep sagaStep, String correlationId, Throwable cause) {
        sagaReplyPort.publishFailure(sagaId, sagaStep, correlationId, cause.getMessage());
    }
}
