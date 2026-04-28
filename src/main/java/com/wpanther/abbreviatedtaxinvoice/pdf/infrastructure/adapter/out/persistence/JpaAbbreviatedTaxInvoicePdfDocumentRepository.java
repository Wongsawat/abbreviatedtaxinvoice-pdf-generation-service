package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.persistence;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

@Repository
public interface JpaAbbreviatedTaxInvoicePdfDocumentRepository
        extends JpaRepository<AbbreviatedTaxInvoicePdfDocumentEntity, UUID> {

    Optional<AbbreviatedTaxInvoicePdfDocumentEntity> findByAbbreviatedTaxInvoiceId(String abbreviatedTaxInvoiceId);

    @Query("SELECT e.documentPath FROM AbbreviatedTaxInvoicePdfDocumentEntity e WHERE e.documentPath IS NOT NULL")
    Set<String> findAllDocumentPaths();

    @Query("SELECT e.documentPath FROM AbbreviatedTaxInvoicePdfDocumentEntity e " +
           "WHERE e.status = 'COMPLETED' AND e.completedAt < :cutoff AND e.documentPath IS NOT NULL")
    List<String> findOrphanedS3Keys(LocalDateTime cutoff);

    @Modifying
    @Query("UPDATE AbbreviatedTaxInvoicePdfDocumentEntity e SET e.documentPath = NULL WHERE e.documentPath = :s3Key")
    void markS3KeyDeleted(String s3Key);
}
