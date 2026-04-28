package com.wpanther.abbreviatedtaxinvoice.pdf.domain.repository;

import com.wpanther.abbreviatedtaxinvoice.pdf.domain.model.AbbreviatedTaxInvoicePdfDocument;
import java.util.Optional;
import java.util.UUID;

public interface AbbreviatedTaxInvoicePdfDocumentRepository {

    AbbreviatedTaxInvoicePdfDocument save(AbbreviatedTaxInvoicePdfDocument document);

    Optional<AbbreviatedTaxInvoicePdfDocument> findById(UUID id);

    Optional<AbbreviatedTaxInvoicePdfDocument> findByAbbreviatedTaxInvoiceId(String abbreviatedTaxInvoiceId);

    void deleteById(UUID id);

    void flush();
}
