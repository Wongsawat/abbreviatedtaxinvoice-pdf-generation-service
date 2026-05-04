package com.wpanther.abbreviatedtaxinvoice.pdf.application.port.out;

import com.wpanther.abbreviatedtaxinvoice.pdf.application.dto.event.DocumentArchiveEvent;

public interface DocumentArchivePort {
    void publish(DocumentArchiveEvent event);
}