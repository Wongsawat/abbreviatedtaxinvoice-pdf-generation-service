package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.persistence;

import com.wpanther.abbreviatedtaxinvoice.pdf.domain.model.AbbreviatedTaxInvoicePdfDocument;
import com.wpanther.abbreviatedtaxinvoice.pdf.domain.repository.AbbreviatedTaxInvoicePdfDocumentRepository;
import java.util.Optional;
import java.util.UUID;

public class AbbreviatedTaxInvoicePdfDocumentRepositoryAdapter implements AbbreviatedTaxInvoicePdfDocumentRepository {

    private final JpaAbbreviatedTaxInvoicePdfDocumentRepository jpaRepository;

    public AbbreviatedTaxInvoicePdfDocumentRepositoryAdapter(JpaAbbreviatedTaxInvoicePdfDocumentRepository jpaRepository) {
        this.jpaRepository = jpaRepository;
    }

    @Override
    public AbbreviatedTaxInvoicePdfDocument save(AbbreviatedTaxInvoicePdfDocument document) {
        return toDomain(jpaRepository.save(toEntity(document)));
    }

    @Override
    public Optional<AbbreviatedTaxInvoicePdfDocument> findById(UUID id) {
        return jpaRepository.findById(id).map(this::toDomain);
    }

    @Override
    public Optional<AbbreviatedTaxInvoicePdfDocument> findByAbbreviatedTaxInvoiceId(String abbreviatedTaxInvoiceId) {
        return jpaRepository.findByAbbreviatedTaxInvoiceId(abbreviatedTaxInvoiceId).map(this::toDomain);
    }

    @Override
    public void deleteById(UUID id) {
        jpaRepository.deleteById(id);
    }

    @Override
    public void flush() {
        jpaRepository.flush();
    }

    private AbbreviatedTaxInvoicePdfDocumentEntity toEntity(AbbreviatedTaxInvoicePdfDocument doc) {
        return AbbreviatedTaxInvoicePdfDocumentEntity.builder()
                .id(doc.getId())
                .abbreviatedTaxInvoiceId(doc.getAbbreviatedTaxInvoiceId())
                .abbreviatedTaxInvoiceNumber(doc.getAbbreviatedTaxInvoiceNumber())
                .documentPath(doc.getDocumentPath())
                .documentUrl(doc.getDocumentUrl())
                .fileSize(doc.getFileSize())
                .mimeType(doc.getMimeType())
                .xmlEmbedded(doc.isXmlEmbedded())
                .status(doc.getStatus())
                .errorMessage(doc.getErrorMessage())
                .retryCount(doc.getRetryCount())
                .createdAt(doc.getCreatedAt() != null ? java.time.LocalDateTime.ofInstant(doc.getCreatedAt(), java.time.ZoneOffset.UTC) : null)
                .completedAt(doc.getCompletedAt() != null ? java.time.LocalDateTime.ofInstant(doc.getCompletedAt(), java.time.ZoneOffset.UTC) : null)
                .build();
    }

    private AbbreviatedTaxInvoicePdfDocument toDomain(AbbreviatedTaxInvoicePdfDocumentEntity e) {
        return new AbbreviatedTaxInvoicePdfDocument(
                e.getAbbreviatedTaxInvoiceId(),
                e.getAbbreviatedTaxInvoiceNumber()
        );
    }
}
