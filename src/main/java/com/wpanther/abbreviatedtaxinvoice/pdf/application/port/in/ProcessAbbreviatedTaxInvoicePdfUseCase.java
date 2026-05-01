package com.wpanther.abbreviatedtaxinvoice.pdf.application.port.in;

import com.wpanther.saga.domain.enums.SagaStep;

/**
 * Inbound port for abbreviated tax invoice PDF generation.
 * Called by SagaCommandHandler (in infrastructure) with plain fields — no command objects.
 */
public interface ProcessAbbreviatedTaxInvoicePdfUseCase {

    void handle(String documentId, String documentNumber, String signedXmlUrl,
                String sagaId, SagaStep sagaStep, String correlationId);
}