package com.wpanther.abbreviatedtaxinvoice.pdf.domain.model;

import com.wpanther.abbreviatedtaxinvoice.pdf.domain.constants.PdfGenerationConstants;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("AbbreviatedTaxInvoicePdfDocument Tests")
class AbbreviatedTaxInvoicePdfDocumentTest {

    @Test
    @DisplayName("new document has PENDING status")
    void testNewDocumentHasPendingStatus() {
        AbbreviatedTaxInvoicePdfDocument doc = new AbbreviatedTaxInvoicePdfDocument("ATINV-001", "ATINV-001");

        assertThat(doc.getStatus()).isEqualTo(GenerationStatus.PENDING);
        assertThat(doc.getAbbreviatedTaxInvoiceId()).isEqualTo("ATINV-001");
        assertThat(doc.getAbbreviatedTaxInvoiceNumber()).isEqualTo("ATINV-001");
        assertThat(doc.getRetryCount()).isEqualTo(0);
        assertThat(doc.isPending()).isTrue();
        assertThat(doc.isCompleted()).isFalse();
        assertThat(doc.isFailed()).isFalse();
    }

    @Test
    @DisplayName("startGeneration() transitions from PENDING to GENERATING")
    void testStartGeneration() {
        AbbreviatedTaxInvoicePdfDocument doc = new AbbreviatedTaxInvoicePdfDocument("ATINV-001", "ATINV-001");

        doc.startGeneration();

        assertThat(doc.getStatus()).isEqualTo(GenerationStatus.GENERATING);
        assertThat(doc.isGenerating()).isTrue();
    }

    @Test
    @DisplayName("startGeneration() throws from non-PENDING status")
    void testStartGenerationFromInvalidState() {
        AbbreviatedTaxInvoicePdfDocument doc = new AbbreviatedTaxInvoicePdfDocument("ATINV-001", "ATINV-001");
        doc.startGeneration();

        assertThatThrownBy(doc::startGeneration)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Cannot start generation from GENERATING");
    }

    @Test
    @DisplayName("markCompleted() transitions from GENERATING to COMPLETED")
    void testMarkCompleted() {
        AbbreviatedTaxInvoicePdfDocument doc = new AbbreviatedTaxInvoicePdfDocument("ATINV-001", "ATINV-001");
        doc.startGeneration();

        doc.markCompleted("s3://bucket/path.pdf", "http://example.com/path.pdf", 1024L);

        assertThat(doc.getStatus()).isEqualTo(GenerationStatus.COMPLETED);
        assertThat(doc.getDocumentPath()).isEqualTo("s3://bucket/path.pdf");
        assertThat(doc.getDocumentUrl()).isEqualTo("http://example.com/path.pdf");
        assertThat(doc.getFileSize()).isEqualTo(1024L);
        assertThat(doc.isCompleted()).isTrue();
        assertThat(doc.getCompletedAt()).isNotNull();
    }

    @Test
    @DisplayName("markCompleted() throws from non-GENERATING status")
    void testMarkCompletedFromInvalidState() {
        AbbreviatedTaxInvoicePdfDocument doc = new AbbreviatedTaxInvoicePdfDocument("ATINV-001", "ATINV-001");

        assertThatThrownBy(() -> doc.markCompleted("path", "url", 100L))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Cannot complete from PENDING");
    }

    @Test
    @DisplayName("markFailed() transitions to FAILED status")
    void testMarkFailed() {
        AbbreviatedTaxInvoicePdfDocument doc = new AbbreviatedTaxInvoicePdfDocument("ATINV-001", "ATINV-001");

        doc.markFailed("Something went wrong");

        assertThat(doc.getStatus()).isEqualTo(GenerationStatus.FAILED);
        assertThat(doc.getErrorMessage()).isEqualTo("Something went wrong");
        assertThat(doc.isFailed()).isTrue();
    }

    @Test
    @DisplayName("markCompensated() transitions to COMPENSATED status")
    void testMarkCompensated() {
        AbbreviatedTaxInvoicePdfDocument doc = new AbbreviatedTaxInvoicePdfDocument("ATINV-001", "ATINV-001");

        doc.markCompensated();

        assertThat(doc.getStatus()).isEqualTo(GenerationStatus.COMPENSATED);
    }

    @Test
    @DisplayName("incrementRetry() increases retry count")
    void testIncrementRetry() {
        AbbreviatedTaxInvoicePdfDocument doc = new AbbreviatedTaxInvoicePdfDocument("ATINV-001", "ATINV-001");

        doc.incrementRetry();
        doc.incrementRetry();
        doc.incrementRetry();

        assertThat(doc.getRetryCount()).isEqualTo(3);
    }

    @Test
    @DisplayName("hasExceededMaxRetries() returns false when below threshold")
    void testHasNotExceededMaxRetries() {
        AbbreviatedTaxInvoicePdfDocument doc = new AbbreviatedTaxInvoicePdfDocument("ATINV-001", "ATINV-001");

        doc.incrementRetry();
        doc.incrementRetry();

        assertThat(doc.hasExceededMaxRetries()).isFalse();
    }

    @Test
    @DisplayName("hasExceededMaxRetries() returns true when at threshold")
    void testHasExceededMaxRetries() {
        AbbreviatedTaxInvoicePdfDocument doc = new AbbreviatedTaxInvoicePdfDocument("ATINV-001", "ATINV-001");

        for (int i = 0; i < PdfGenerationConstants.DEFAULT_MAX_RETRIES; i++) {
            doc.incrementRetry();
        }

        assertThat(doc.hasExceededMaxRetries()).isTrue();
    }

    @Test
    @DisplayName("document has default MIME type and XML embedded flag")
    void testDefaultMimeTypeAndXmlEmbedded() {
        AbbreviatedTaxInvoicePdfDocument doc = new AbbreviatedTaxInvoicePdfDocument("ATINV-001", "ATINV-001");

        assertThat(doc.getMimeType()).isEqualTo("application/pdf");
        assertThat(doc.isXmlEmbedded()).isTrue();
    }
}