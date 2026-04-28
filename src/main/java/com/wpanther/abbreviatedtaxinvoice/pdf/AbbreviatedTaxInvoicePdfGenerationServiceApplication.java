package com.wpanther.abbreviatedtaxinvoice.pdf;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class AbbreviatedTaxInvoicePdfGenerationServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(AbbreviatedTaxInvoicePdfGenerationServiceApplication.class, args);
    }
}
