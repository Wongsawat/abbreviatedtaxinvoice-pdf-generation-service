package com.wpanther.abbreviatedtaxinvoice.pdf.domain.service;

public interface AbbreviatedTaxInvoicePdfGenerationService {

    /**
     * Generate PDF/A-3 from the signed XML document.
     *
     * @param abbreviatedTaxInvoiceNumber document number (used for logging and file naming)
     * @param signedXml full Thai e-Tax signed XML (rsm:AbbreviatedTaxInvoice_CrossIndustryInvoice)
     * @return PDF/A-3 bytes with the signed XML embedded as an attachment
     * @throws AbbreviatedTaxInvoicePdfGenerationException if generation fails
     */
    byte[] generatePdf(String abbreviatedTaxInvoiceNumber, String signedXml)
        throws AbbreviatedTaxInvoicePdfGenerationException;

    class AbbreviatedTaxInvoicePdfGenerationException extends Exception {
        public AbbreviatedTaxInvoicePdfGenerationException(String message) { super(message); }
        public AbbreviatedTaxInvoicePdfGenerationException(String message, Throwable cause) { super(message, cause); }
    }
}
