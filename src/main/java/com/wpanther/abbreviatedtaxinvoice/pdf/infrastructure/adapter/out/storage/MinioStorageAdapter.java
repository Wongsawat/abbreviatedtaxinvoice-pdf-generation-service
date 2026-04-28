package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.storage;

import com.wpanther.abbreviatedtaxinvoice.pdf.application.port.out.PdfStoragePort;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetUrlRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.time.Instant;
import java.util.UUID;

@Component
@Slf4j
public class MinioStorageAdapter implements PdfStoragePort {

    private final S3Client s3Client;
    private final String bucketName;
    private final String endpoint;

    public MinioStorageAdapter(
            S3Client s3Client,
            @Value("${app.minio.bucket-name}") String bucketName,
            @Value("${app.minio.endpoint}") String endpoint) {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.endpoint = endpoint;
        log.info("MinioStorageAdapter initialized: bucket={} endpoint={}", bucketName, endpoint);
    }

    @Override
    public String store(String abbreviatedTaxInvoiceNumber, byte[] pdfBytes) {
        String timestamp = String.valueOf(Instant.now().toEpochMilli());
        String uuid = UUID.randomUUID().toString().substring(0, 8);
        String s3Key = String.format("abbreviated-tax-invoice/%s/%s_%s.pdf",
                abbreviatedTaxInvoiceNumber, timestamp, uuid);

        log.debug("Storing PDF to MinIO: bucket={} key={} size={} bytes",
                bucketName, s3Key, pdfBytes.length);

        try {
            PutObjectRequest putRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(s3Key)
                    .contentType("application/pdf")
                    .contentLength((long) pdfBytes.length)
                    .build();

            s3Client.putObject(putRequest, RequestBody.fromBytes(pdfBytes));
            log.info("PDF stored successfully: s3Key={} size={} bytes", s3Key, pdfBytes.length);
            return s3Key;
        } catch (Exception e) {
            log.error("Failed to store PDF to MinIO: {}", s3Key, e);
            throw new RuntimeException("Failed to store PDF to MinIO: " + e.getMessage(), e);
        }
    }

    @Override
    public void delete(String s3Key) {
        log.debug("Deleting PDF from MinIO: bucket={} key={}", bucketName, s3Key);
        try {
            DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder()
                    .bucket(bucketName)
                    .key(s3Key)
                    .build();
            s3Client.deleteObject(deleteRequest);
            log.info("PDF deleted successfully: s3Key={}", s3Key);
        } catch (Exception e) {
            log.error("Failed to delete PDF from MinIO: {}", s3Key, e);
            throw new RuntimeException("Failed to delete PDF from MinIO: " + e.getMessage(), e);
        }
    }

    @Override
    public String resolveUrl(String s3Key) {
        try {
            GetUrlRequest request = GetUrlRequest.builder()
                    .bucket(bucketName)
                    .key(s3Key)
                    .build();
            String url = s3Client.utilities().getUrl(request).toString();
            log.debug("Resolved MinIO URL: key={} url={}", s3Key, url);
            return url;
        } catch (Exception e) {
            log.error("Failed to resolve MinIO URL for key: {}", s3Key, e);
            throw new RuntimeException("Failed to resolve MinIO URL: " + e.getMessage(), e);
        }
    }
}