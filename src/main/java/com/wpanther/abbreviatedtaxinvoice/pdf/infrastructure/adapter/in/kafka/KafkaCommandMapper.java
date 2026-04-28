package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.in.kafka;

import org.springframework.stereotype.Component;

@Component
public class KafkaCommandMapper {

    public KafkaAbbreviatedTaxInvoiceProcessCommand toProcess(KafkaAbbreviatedTaxInvoiceProcessCommand src) {
        return src;
    }

    public KafkaAbbreviatedTaxInvoiceCompensateCommand toCompensate(KafkaAbbreviatedTaxInvoiceCompensateCommand src) {
        return src;
    }
}
