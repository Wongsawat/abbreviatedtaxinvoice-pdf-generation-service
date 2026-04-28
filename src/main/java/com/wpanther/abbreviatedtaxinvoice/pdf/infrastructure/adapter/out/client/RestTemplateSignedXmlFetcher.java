package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.client;

import com.wpanther.abbreviatedtaxinvoice.pdf.application.port.out.SignedXmlFetchPort;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

@Component
@Slf4j
public class RestTemplateSignedXmlFetcher implements SignedXmlFetchPort {

    private final RestTemplate restTemplate;
    private final String baseUrl;

    public RestTemplateSignedXmlFetcher(
            RestTemplate restTemplate,
            @Value("${app.document-storage.base-url:http://localhost:8084}") String baseUrl) {
        this.restTemplate = restTemplate;
        this.baseUrl = baseUrl;
        log.info("RestTemplateSignedXmlFetcher initialized: baseUrl={}", baseUrl);
    }

    @Override
    public String fetch(String url) throws SignedXmlFetchException {
        log.debug("Fetching signed XML from: {}", url);

        if (url == null || url.isBlank()) {
            throw new SignedXmlFetchException("url is null or blank");
        }

        try {
            String xml = restTemplate.getForObject(url, String.class);
            if (xml == null || xml.isBlank()) {
                throw new SignedXmlFetchException("Empty XML response from: " + url);
            }
            log.debug("Fetched signed XML: {} bytes", xml.length());
            return xml;
        } catch (org.springframework.web.client.HttpClientErrorException e) {
            log.error("HTTP error fetching signed XML from {}: {}", url, e.getStatusCode());
            throw new SignedXmlFetchException("HTTP " + e.getStatusCode() + " from " + url, e);
        } catch (Exception e) {
            log.error("Failed to fetch signed XML from {}: {}", url, e.getMessage());
            throw new SignedXmlFetchException("Failed to fetch signed XML: " + e.getMessage(), e);
        }
    }

    public static class SignedXmlFetchException extends RuntimeException {
        public SignedXmlFetchException(String message) {
            super(message);
        }

        public SignedXmlFetchException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}