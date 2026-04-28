package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.config;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

@Configuration
@Slf4j
public class RestTemplateConfig {

    @Value("${app.rest-client.connect-timeout:5000}") private int connectTimeout;
    @Value("${app.rest-client.read-timeout:30000}") private int readTimeout;

    private final CircuitBreakerRegistry circuitBreakerRegistry;

    public RestTemplateConfig(CircuitBreakerRegistry circuitBreakerRegistry) {
        this.circuitBreakerRegistry = circuitBreakerRegistry;
    }

    @PostConstruct
    public void setupCircuitBreakerLogging() {
        CircuitBreaker signedXmlFetch = circuitBreakerRegistry.circuitBreaker("signedXmlFetch");
        CircuitBreaker minio = circuitBreakerRegistry.circuitBreaker("minio");
        signedXmlFetch.getEventPublisher().onStateTransition(e ->
                log.info("Circuit breaker 'signedXmlFetch' state transition: {}", e.getStateTransition()));
        minio.getEventPublisher().onStateTransition(e ->
                log.info("Circuit breaker 'minio' state transition: {}", e.getStateTransition()));
        log.info("Circuit breaker event logging configured");
    }

    @Bean
    @ConditionalOnMissingBean(RestTemplate.class)
    public RestTemplate restTemplate() {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(connectTimeout);
        factory.setReadTimeout(readTimeout);
        log.info("Configured RestTemplate: connectTimeout={}ms, readTimeout={}ms", connectTimeout, readTimeout);
        return new RestTemplate(factory);
    }
}
