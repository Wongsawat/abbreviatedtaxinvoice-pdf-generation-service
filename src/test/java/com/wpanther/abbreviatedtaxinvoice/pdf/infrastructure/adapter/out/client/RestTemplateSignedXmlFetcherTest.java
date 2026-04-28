package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.client;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("RestTemplateSignedXmlFetcher Tests")
class RestTemplateSignedXmlFetcherTest {

    @Mock
    private RestTemplate restTemplate;

    private RestTemplateSignedXmlFetcher fetcher;

    @BeforeEach
    void setUp() {
        fetcher = new RestTemplateSignedXmlFetcher(restTemplate, "http://localhost:8084");
    }

    @Test
    @DisplayName("fetch() returns XML content from URL")
    void testFetchSuccess() {
        String xmlContent = "<test>XML content</test>";
        when(restTemplate.getForObject("http://localhost:8084/test.xml", String.class))
                .thenReturn(xmlContent);

        String result = fetcher.fetch("http://localhost:8084/test.xml");

        assertThat(result).isEqualTo(xmlContent);
    }

    @Test
    @DisplayName("fetch() throws exception for null URL")
    void testFetchNullUrl() {
        assertThatThrownBy(() -> fetcher.fetch(null))
                .isInstanceOf(RestTemplateSignedXmlFetcher.SignedXmlFetchException.class)
                .hasMessageContaining("null or blank");
    }

    @Test
    @DisplayName("fetch() throws exception for blank URL")
    void testFetchBlankUrl() {
        assertThatThrownBy(() -> fetcher.fetch("   "))
                .isInstanceOf(RestTemplateSignedXmlFetcher.SignedXmlFetchException.class)
                .hasMessageContaining("null or blank");
    }

    @Test
    @DisplayName("fetch() throws exception for empty response")
    void testFetchEmptyResponse() {
        when(restTemplate.getForObject(any(String.class), eq(String.class)))
                .thenReturn("");

        assertThatThrownBy(() -> fetcher.fetch("http://localhost:8084/empty.xml"))
                .isInstanceOf(RestTemplateSignedXmlFetcher.SignedXmlFetchException.class)
                .hasMessageContaining("Empty XML response");
    }

    @Test
    @DisplayName("fetch() throws exception for HTTP error")
    void testFetchHttpError() {
        when(restTemplate.getForObject(any(String.class), eq(String.class)))
                .thenThrow(new org.springframework.web.client.HttpClientErrorException(
                        org.springframework.http.HttpStatus.NOT_FOUND));

        assertThatThrownBy(() -> fetcher.fetch("http://localhost:8084/notfound.xml"))
                .isInstanceOf(RestTemplateSignedXmlFetcher.SignedXmlFetchException.class)
                .hasMessageContaining("HTTP 404");
    }
}