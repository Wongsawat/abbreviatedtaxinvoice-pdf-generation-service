package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
@Slf4j
@ConditionalOnProperty(name = "app.fonts.health-check.enabled", havingValue = "true", matchIfMissing = true)
public class FontHealthCheck {

    private static final String[] REQUIRED_FONTS = {
            "fonts/THSarabunNew.ttf",
            "fonts/THSarabunNew-Bold.ttf",
            "fonts/THSarabunNew-Italic.ttf",
            "fonts/THSarabunNew-BoldItalic.ttf",
            "fonts/NotoSansThaiLooped-Regular.ttf",
            "fonts/NotoSansThaiLooped-Bold.ttf"
    };

    @Value("${app.fonts.health-check.fail-on-error:true}")
    private boolean failOnError;

    @EventListener(ApplicationReadyEvent.class)
    public void checkFontsAtStartup() {
        log.info("Performing font health check...");
        List<String> missing = new ArrayList<>();
        for (String fontPath : REQUIRED_FONTS) {
            if (!new ClassPathResource(fontPath).exists()) {
                missing.add(fontPath);
                log.warn("Required font missing: {}", fontPath);
            }
        }
        if (missing.isEmpty()) {
            log.info("Font health check passed: all {} fonts present", REQUIRED_FONTS.length);
            return;
        }
        String msg = String.format("Missing %d of %d required Thai fonts: %s. "
                + "Place fonts in src/main/resources/fonts/ or disable check with "
                + "app.fonts.health-check.enabled=false.",
                missing.size(), REQUIRED_FONTS.length, missing);
        if (failOnError) {
            log.error("Font health check failed: {}", msg);
            throw new IllegalStateException(msg);
        }
        log.warn("Font health check failed but continuing: {}", msg);
    }
}
