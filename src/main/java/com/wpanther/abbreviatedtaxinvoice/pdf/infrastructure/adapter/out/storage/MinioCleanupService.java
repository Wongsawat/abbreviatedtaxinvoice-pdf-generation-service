package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.storage;

import com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.persistence.AbbreviatedTaxInvoicePdfDocumentRepositoryAdapter;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

@Service
@Slf4j
public class MinioCleanupService {

    private final AbbreviatedTaxInvoicePdfDocumentRepositoryAdapter documentRepository;
    private final MinioStorageAdapter storageAdapter;
    private final Counter cleanupCounter;

    public MinioCleanupService(
            AbbreviatedTaxInvoicePdfDocumentRepositoryAdapter documentRepository,
            MinioStorageAdapter storageAdapter,
            MeterRegistry meterRegistry) {
        this.documentRepository = documentRepository;
        this.storageAdapter = storageAdapter;
        this.cleanupCounter = Counter.builder("pdf.cleanup.deleted")
                .description("Number of PDF files cleaned up")
                .register(meterRegistry);
    }

    @Scheduled(cron = "${app.pdf.cleanup.cron:0 0 2 * * ?}")
    public void cleanup() {
        log.info("Starting PDF cleanup job...");

        LocalDateTime cutoff = LocalDateTime.now(ZoneId.systemDefault())
                .minusDays(30);

        List<String> orphanedKeys = documentRepository.findOrphanedS3Keys(cutoff);
        int deletedCount = 0;

        for (String s3Key : orphanedKeys) {
            try {
                storageAdapter.delete(s3Key);
                documentRepository.markS3KeyDeleted(s3Key);
                deletedCount++;
                cleanupCounter.increment();
                log.debug("Cleaned up orphaned PDF: {}", s3Key);
            } catch (Exception e) {
                log.warn("Failed to clean up PDF: {}: {}", s3Key, e.getMessage());
            }
        }

        log.info("PDF cleanup completed: {} files deleted", deletedCount);
    }
}