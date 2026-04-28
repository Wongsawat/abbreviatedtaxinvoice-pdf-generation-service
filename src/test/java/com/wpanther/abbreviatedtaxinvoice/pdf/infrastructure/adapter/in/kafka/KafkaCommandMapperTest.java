package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.in.kafka;

import com.wpanther.saga.domain.enums.SagaStep;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class KafkaCommandMapperTest {

    private final KafkaCommandMapper mapper = new KafkaCommandMapper();

    @Test
    void toProcess_mapsAllFields() {
        var src = new KafkaAbbreviatedTaxInvoiceProcessCommand(
                "saga-1", SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF, "corr-1",
                "doc-1", "423-612", "http://minio/xml");

        var result = mapper.toProcess(src);

        assertThat(result.getSagaId()).isEqualTo("saga-1");
        assertThat(result.getCorrelationId()).isEqualTo("corr-1");
        assertThat(result.getDocumentId()).isEqualTo("doc-1");
        assertThat(result.getDocumentNumber()).isEqualTo("423-612");
        assertThat(result.getSignedXmlUrl()).isEqualTo("http://minio/xml");
    }

    @Test
    void toCompensate_mapsAllFields() {
        var src = new KafkaAbbreviatedTaxInvoiceCompensateCommand(
                "saga-2", SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF, "corr-2", "doc-2");

        var result = mapper.toCompensate(src);

        assertThat(result.getSagaId()).isEqualTo("saga-2");
        assertThat(result.getCorrelationId()).isEqualTo("corr-2");
        assertThat(result.getDocumentId()).isEqualTo("doc-2");
    }
}
