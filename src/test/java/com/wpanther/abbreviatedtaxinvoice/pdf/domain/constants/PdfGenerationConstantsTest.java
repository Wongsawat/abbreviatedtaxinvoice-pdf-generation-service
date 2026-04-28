package com.wpanther.abbreviatedtaxinvoice.pdf.domain.constants;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class PdfGenerationConstantsTest {

    @Test
    void shouldHaveExpectedDefaultMaxRetries() {
        assertThat(PdfGenerationConstants.DEFAULT_MAX_RETRIES).isEqualTo(3);
    }

    @Test
    void shouldHaveExpectedPdfMimeType() {
        assertThat(PdfGenerationConstants.PDF_MIME_TYPE).isEqualTo("application/pdf");
    }
}
