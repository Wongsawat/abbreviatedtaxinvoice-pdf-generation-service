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
    @DisplayName("checkFontsAtStartup() throws when fonts are missing and failOnError=true")
    void testFontsAbsent_FailOnError() {
        FontHealthCheck check = healthCheck(true);
        assertThatThrownBy(check::checkFontsAtStartup)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Missing");
    }

    @Test
    @DisplayName("checkFontsAtStartup() logs warning only when fonts are missing and failOnError=false")
    void testFontsAbsent_WarnOnly() {
        FontHealthCheck check = healthCheck(false);
        assertThatCode(check::checkFontsAtStartup).doesNotThrowAnyException();
    }
}
