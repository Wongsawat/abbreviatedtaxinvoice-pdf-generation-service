package com.wpanther.abbreviatedtaxinvoice.pdf.domain.exception;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class AbbreviatedTaxInvoicePdfGenerationExceptionTest {

    @Test
    void shouldCreateWithMessage() {
        var ex = new AbbreviatedTaxInvoicePdfGenerationException("test error");
        assertThat(ex.getMessage()).isEqualTo("test error");
        assertThat(ex).isInstanceOf(RuntimeException.class);
    }

    @Test
    void shouldCreateWithMessageAndCause() {
        var cause = new IllegalStateException("root cause");
        var ex = new AbbreviatedTaxInvoicePdfGenerationException("wrapped", cause);
        assertThat(ex.getMessage()).isEqualTo("wrapped");
        assertThat(ex.getCause()).isSameAs(cause);
    }
}
