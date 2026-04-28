package com.wpanther.abbreviatedtaxinvoice.pdf.application.port.out;

public interface PdfStoragePort {
    String store(String abbreviatedTaxInvoiceNumber, byte[] pdfBytes);
    void delete(String s3Key);
    String resolveUrl(String s3Key);
}
