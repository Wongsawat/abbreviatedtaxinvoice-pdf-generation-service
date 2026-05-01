package com.wpanther.abbreviatedtaxinvoice.pdf.application.port.in;

import com.wpanther.saga.domain.enums.SagaStep;

/**
 * Inbound port for abbreviated tax invoice PDF compensation.
 * Called by SagaCommandHandler (in infrastructure) with plain fields — no command objects.
 */
public interface CompensateAbbreviatedTaxInvoicePdfUseCase {

    void handle(String documentId, String sagaId, SagaStep sagaStep, String correlationId);
}
