package com.wpanther.abbreviatedtaxinvoice.pdf.domain.model;

import com.wpanther.abbreviatedtaxinvoice.pdf.domain.constants.PdfGenerationConstants;
import java.time.Instant;
import java.util.UUID;

/**
 * Aggregate root for abbreviated tax invoice PDF documents.
 * State machine: PENDING -> GENERATING -> COMPLETED | FAILED
 */
public class AbbreviatedTaxInvoicePdfDocument {

    private UUID id;
    private String abbreviatedTaxInvoiceId;
    private String abbreviatedTaxInvoiceNumber;
    private GenerationStatus status;
    private String documentPath;
    private String documentUrl;
    private Long fileSize;
    private String mimeType;
    private boolean xmlEmbedded;
    private String errorMessage;
    private int retryCount;
    private Instant createdAt;
    private Instant updatedAt;
    private Instant completedAt;

    public AbbreviatedTaxInvoicePdfDocument(String abbreviatedTaxInvoiceId, String abbreviatedTaxInvoiceNumber) {
        this.id = UUID.randomUUID();
        this.abbreviatedTaxInvoiceId = abbreviatedTaxInvoiceId;
        this.abbreviatedTaxInvoiceNumber = abbreviatedTaxInvoiceNumber;
        this.status = GenerationStatus.PENDING;
        this.retryCount = 0;
        this.createdAt = Instant.now();
        this.updatedAt = Instant.now();
        this.mimeType = "application/pdf";
        this.xmlEmbedded = true;
    }

    public void startGeneration() {
        if (this.status != GenerationStatus.PENDING) {
            throw new IllegalStateException("Cannot start generation from " + this.status);
        }
        this.status = GenerationStatus.GENERATING;
        this.updatedAt = Instant.now();
    }

    public void markCompleted(String documentPath, String documentUrl, Long fileSize) {
        if (this.status != GenerationStatus.GENERATING) {
            throw new IllegalStateException("Cannot complete from " + this.status);
        }
        this.documentPath = documentPath;
        this.documentUrl = documentUrl;
        this.fileSize = fileSize;
        this.status = GenerationStatus.COMPLETED;
        this.completedAt = Instant.now();
        this.updatedAt = Instant.now();
    }

    public void markFailed(String errorMessage) {
        this.errorMessage = errorMessage;
        this.status = GenerationStatus.FAILED;
        this.updatedAt = Instant.now();
    }

    public void incrementRetry() {
        this.retryCount++;
        this.updatedAt = Instant.now();
    }

    public boolean hasExceededMaxRetries() {
        return this.retryCount >= PdfGenerationConstants.DEFAULT_MAX_RETRIES;
    }

    public UUID getId() { return id; }
    public String getAbbreviatedTaxInvoiceId() { return abbreviatedTaxInvoiceId; }
    public String getAbbreviatedTaxInvoiceNumber() { return abbreviatedTaxInvoiceNumber; }
    public GenerationStatus getStatus() { return status; }
    public String getDocumentPath() { return documentPath; }
    public String getDocumentUrl() { return documentUrl; }
    public Long getFileSize() { return fileSize != null ? fileSize : 0L; }
    public String getMimeType() { return mimeType; }
    public boolean isXmlEmbedded() { return xmlEmbedded; }
    public String getErrorMessage() { return errorMessage; }
    public int getRetryCount() { return retryCount; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
    public Instant getCompletedAt() { return completedAt; }

    public boolean isPending() { return status == GenerationStatus.PENDING; }
    public boolean isGenerating() { return status == GenerationStatus.GENERATING; }
    public boolean isCompleted() { return status == GenerationStatus.COMPLETED; }
    public boolean isFailed() { return status == GenerationStatus.FAILED; }
}
