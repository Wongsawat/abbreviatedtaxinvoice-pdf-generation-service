package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

@DisplayName("FontHealthCheck Tests")
class FontHealthCheckTest {

    private FontHealthCheck healthCheck(boolean failOnError) {
        FontHealthCheck check = new FontHealthCheck();
        try {
            var field = FontHealthCheck.class.getDeclaredField("failOnError");
            field.setAccessible(true);
            field.set(check, failOnError);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return check;
    }

    @Test
    @DisplayName("checkFontsAtStartup() completes when all required fonts are present")
    void testFontsPresent_NoException() {
        FontHealthCheck check = healthCheck(false);
        assertThatCode(check::checkFontsAtStartup).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("checkFontsAtStartup() completes even with failOnError=true when fonts are present")
    void testFontsPresent_FailOnError() {
        FontHealthCheck check = healthCheck(true);
        assertThatCode(check::checkFontsAtStartup).doesNotThrowAnyException();
    }
}