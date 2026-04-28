package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.persistence;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

@Repository
public interface JpaAbbreviatedTaxInvoicePdfDocumentRepository
        extends JpaRepository<AbbreviatedTaxInvoicePdfDocumentEntity, UUID> {

    Optional<AbbreviatedTaxInvoicePdfDocumentEntity> findByAbbreviatedTaxInvoiceId(String abbreviatedTaxInvoiceId);

    @Query("SELECT e.documentPath FROM AbbreviatedTaxInvoicePdfDocumentEntity e WHERE e.documentPath IS NOT NULL")
    Set<String> findAllDocumentPaths();
}
