-- abbreviated_tax_invoice_pdf_documents table
CREATE TABLE abbreviated_tax_invoice_pdf_documents (
    id UUID PRIMARY KEY,
    abbreviated_tax_invoice_id VARCHAR(100) NOT NULL UNIQUE,
    abbreviated_tax_invoice_number VARCHAR(50) NOT NULL,
    document_path VARCHAR(500),
    document_url VARCHAR(1000),
    file_size BIGINT,
    mime_type VARCHAR(100) NOT NULL DEFAULT 'application/pdf',
    xml_embedded BOOLEAN NOT NULL DEFAULT false,
    status VARCHAR(20) NOT NULL,
    error_message TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP
);

CREATE INDEX idx_ati_pdf_ati_id ON abbreviated_tax_invoice_pdf_documents(abbreviated_tax_invoice_id);
CREATE INDEX idx_ati_pdf_ati_number ON abbreviated_tax_invoice_pdf_documents(abbreviated_tax_invoice_number);
CREATE INDEX idx_ati_pdf_status ON abbreviated_tax_invoice_pdf_documents(status);

-- outbox_events table
CREATE TABLE outbox_events (
    id UUID PRIMARY KEY,
    aggregate_type VARCHAR(100) NOT NULL,
    aggregate_id VARCHAR(100) NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    payload TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    published_at TIMESTAMP WITH TIME ZONE,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    retry_count INTEGER NOT NULL DEFAULT 0,
    error_message VARCHAR(1000),
    topic VARCHAR(255),
    partition_key VARCHAR(255),
    headers TEXT
);

CREATE INDEX idx_outbox_status ON outbox_events(status);
CREATE INDEX idx_outbox_created ON outbox_events(created_at);
CREATE INDEX idx_outbox_status_created ON outbox_events(status, created_at);
CREATE INDEX idx_outbox_aggregate ON outbox_events(aggregate_id, aggregate_type);
