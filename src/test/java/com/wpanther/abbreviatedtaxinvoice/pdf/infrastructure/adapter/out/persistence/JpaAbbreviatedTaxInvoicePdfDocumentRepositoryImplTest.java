package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.persistence;

import com.wpanther.abbreviatedtaxinvoice.pdf.domain.model.AbbreviatedTaxInvoicePdfDocument;
import com.wpanther.abbreviatedtaxinvoice.pdf.domain.model.GenerationStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("AbbreviatedTaxInvoicePdfDocumentRepositoryAdapter Unit Tests")
class JpaAbbreviatedTaxInvoicePdfDocumentRepositoryImplTest {

    @Mock
    private JpaAbbreviatedTaxInvoicePdfDocumentRepository jpaRepository;

    private AbbreviatedTaxInvoicePdfDocumentRepositoryAdapter repository;

    private UUID id;

    @BeforeEach
    void setUp() {
        repository = new AbbreviatedTaxInvoicePdfDocumentRepositoryAdapter(jpaRepository);
        id = UUID.randomUUID();
    }

    @Test
    @DisplayName("save() maps domain to entity and back")
    void testSave_roundTrip() {
        AbbreviatedTaxInvoicePdfDocumentEntity entity = new AbbreviatedTaxInvoicePdfDocumentEntity();
        entity.setId(id);
        entity.setAbbreviatedTaxInvoiceId("ati-123");
        entity.setAbbreviatedTaxInvoiceNumber("423-612");
        entity.setDocumentPath("2026/04/28/abbreviated-tax-invoice-423-612-abc.pdf");
        entity.setDocumentUrl("http://localhost:9000/abbreviatedtaxinvoices/2026/04/28/abbreviated-tax-invoice-423-612-abc.pdf");
        entity.setFileSize(98765L);
        entity.setMimeType("application/pdf");
        entity.setXmlEmbedded(true);
        entity.setStatus(GenerationStatus.COMPLETED);
        entity.setRetryCount(1);
        entity.setCreatedAt(LocalDateTime.now());
        entity.setCompletedAt(LocalDateTime.now());
        when(jpaRepository.save(any())).thenReturn(entity);

        AbbreviatedTaxInvoicePdfDocument result = repository.save(domain("ati-123", "423-612"));

        verify(jpaRepository).save(any(AbbreviatedTaxInvoicePdfDocumentEntity.class));
        assertThat(result.getAbbreviatedTaxInvoiceId()).isEqualTo("ati-123");
    }

    @Test
    @DisplayName("findById() returns mapped domain when found")
    void testFindById_found() {
        AbbreviatedTaxInvoicePdfDocumentEntity entity = new AbbreviatedTaxInvoicePdfDocumentEntity();
        entity.setId(id);
        entity.setAbbreviatedTaxInvoiceId("ati-123");
        entity.setAbbreviatedTaxInvoiceNumber("423-612");
        entity.setStatus(GenerationStatus.COMPLETED);
        entity.setFileSize(98765L);
        entity.setXmlEmbedded(true);
        entity.setRetryCount(0);
        entity.setMimeType("application/pdf");
        entity.setCreatedAt(LocalDateTime.now());
        when(jpaRepository.findById(id)).thenReturn(Optional.of(entity));

        Optional<AbbreviatedTaxInvoicePdfDocument> result = repository.findById(id);

        assertThat(result).isPresent();
        assertThat(result.get().getAbbreviatedTaxInvoiceId()).isEqualTo("ati-123");
    }

    @Test
    @DisplayName("findById() returns empty when not found")
    void testFindById_notFound() {
        when(jpaRepository.findById(id)).thenReturn(Optional.empty());
        assertThat(repository.findById(id)).isEmpty();
    }

    @Test
    @DisplayName("findByAbbreviatedTaxInvoiceId() returns mapped domain when found")
    void testFindByAbbreviatedTaxInvoiceId_found() {
        AbbreviatedTaxInvoicePdfDocumentEntity entity = new AbbreviatedTaxInvoicePdfDocumentEntity();
        entity.setId(id);
        entity.setAbbreviatedTaxInvoiceId("ati-123");
        entity.setAbbreviatedTaxInvoiceNumber("423-612");
        entity.setStatus(GenerationStatus.PENDING);
        entity.setXmlEmbedded(false);
        entity.setRetryCount(0);
        entity.setMimeType("application/pdf");
        entity.setCreatedAt(LocalDateTime.now());
        when(jpaRepository.findByAbbreviatedTaxInvoiceId("ati-123")).thenReturn(Optional.of(entity));

        Optional<AbbreviatedTaxInvoicePdfDocument> result = repository.findByAbbreviatedTaxInvoiceId("ati-123");

        assertThat(result).isPresent();
        assertThat(result.get().getAbbreviatedTaxInvoiceNumber()).isEqualTo("423-612");
    }

    @Test
    @DisplayName("findByAbbreviatedTaxInvoiceId() returns empty when not found")
    void testFindByAbbreviatedTaxInvoiceId_notFound() {
        when(jpaRepository.findByAbbreviatedTaxInvoiceId("unknown")).thenReturn(Optional.empty());
        assertThat(repository.findByAbbreviatedTaxInvoiceId("unknown")).isEmpty();
    }

    @Test
    @DisplayName("deleteById() delegates to JPA repository")
    void testDeleteById() {
        repository.deleteById(id);
        verify(jpaRepository).deleteById(id);
    }

    @Test
    @DisplayName("flush() delegates to JPA repository")
    void testFlush() {
        repository.flush();
        verify(jpaRepository).flush();
    }

    private static AbbreviatedTaxInvoicePdfDocument domain(String atiId, String number) {
        return new AbbreviatedTaxInvoicePdfDocument(atiId, number);
    }
}
