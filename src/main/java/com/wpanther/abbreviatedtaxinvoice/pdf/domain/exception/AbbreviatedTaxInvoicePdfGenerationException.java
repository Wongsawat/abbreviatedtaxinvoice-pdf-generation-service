package com.wpanther.abbreviatedtaxinvoice.pdf.domain.exception;

public class AbbreviatedTaxInvoicePdfGenerationException extends RuntimeException {

    public AbbreviatedTaxInvoicePdfGenerationException(String message) {
        super(message);
    }

    public AbbreviatedTaxInvoicePdfGenerationException(String message, Throwable cause) {
        super(message, cause);
    }
}
