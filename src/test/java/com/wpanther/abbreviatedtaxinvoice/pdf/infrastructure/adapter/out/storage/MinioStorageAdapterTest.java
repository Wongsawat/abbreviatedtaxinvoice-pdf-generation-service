package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Utilities;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetUrlRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;

import java.net.URL;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("MinioStorageAdapter Tests")
class MinioStorageAdapterTest {

    @Mock
    private S3Client s3Client;

    @Mock
    private S3Utilities s3Utilities;

    private MinioStorageAdapter adapter;

    @BeforeEach
    void setUp() {
        adapter = new MinioStorageAdapter(s3Client, "test-bucket", "http://localhost:9000");
    }

    @Test
    @DisplayName("store() uploads PDF to MinIO and returns S3 key")
    void testStore() {
        byte[] pdfBytes = "PDF content".getBytes();
        when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenReturn(software.amazon.awssdk.services.s3.model.PutObjectResponse.builder().build());

        String s3Key = adapter.store("INV-001", pdfBytes);

        assertThat(s3Key).startsWith("abbreviated-tax-invoice/INV-001/");
        assertThat(s3Key).endsWith(".pdf");

        ArgumentCaptor<PutObjectRequest> requestCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(s3Client).putObject(requestCaptor.capture(), any(RequestBody.class));
        assertThat(requestCaptor.getValue().bucket()).isEqualTo("test-bucket");
        assertThat(requestCaptor.getValue().contentType()).isEqualTo("application/pdf");
    }

    @Test
    @DisplayName("delete() removes PDF from MinIO")
    void testDelete() {
        String s3Key = "abbreviated-tax-invoice/INV-001/test.pdf";
        when(s3Client.deleteObject(any(DeleteObjectRequest.class)))
                .thenReturn(software.amazon.awssdk.services.s3.model.DeleteObjectResponse.builder().build());

        adapter.delete(s3Key);

        ArgumentCaptor<DeleteObjectRequest> requestCaptor = ArgumentCaptor.forClass(DeleteObjectRequest.class);
        verify(s3Client).deleteObject(requestCaptor.capture());
        assertThat(requestCaptor.getValue().bucket()).isEqualTo("test-bucket");
        assertThat(requestCaptor.getValue().key()).isEqualTo(s3Key);
    }

    @Test
    @DisplayName("resolveUrl() returns MinIO URL")
    void testResolveUrl() throws Exception {
        String s3Key = "abbreviated-tax-invoice/INV-001/test.pdf";
        URL expectedUrl = new URL("http://localhost:9000/test-bucket/abbreviated-tax-invoice/INV-001/test.pdf");

        when(s3Client.utilities()).thenReturn(s3Utilities);
        when(s3Utilities.getUrl(any(GetUrlRequest.class))).thenReturn(expectedUrl);

        String url = adapter.resolveUrl(s3Key);

        assertThat(url).isEqualTo(expectedUrl.toString());
        verify(s3Client).utilities();
    }
}