package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.in.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wpanther.abbreviatedtaxinvoice.pdf.application.port.in.CompensateAbbreviatedTaxInvoicePdfUseCase;
import com.wpanther.abbreviatedtaxinvoice.pdf.application.port.in.ProcessAbbreviatedTaxInvoicePdfUseCase;
import com.wpanther.abbreviatedtaxinvoice.pdf.application.service.SagaCommandHandler;
import com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.in.kafka.dto.CompensateAbbreviatedTaxInvoicePdfCommand;
import com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.in.kafka.dto.ProcessAbbreviatedTaxInvoicePdfCommand;
import com.wpanther.saga.domain.enums.SagaStep;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
@Slf4j
public class SagaRouteConfig extends RouteBuilder {

    private final ProcessAbbreviatedTaxInvoicePdfUseCase processUseCase;
    private final CompensateAbbreviatedTaxInvoicePdfUseCase compensateUseCase;
    private final SagaCommandHandler sagaCommandHandler;
    private final ObjectMapper objectMapper;

    public SagaRouteConfig(ProcessAbbreviatedTaxInvoicePdfUseCase processUseCase,
                           CompensateAbbreviatedTaxInvoicePdfUseCase compensateUseCase,
                           SagaCommandHandler sagaCommandHandler,
                           ObjectMapper objectMapper) {
        this.processUseCase = processUseCase;
        this.compensateUseCase = compensateUseCase;
        this.sagaCommandHandler = sagaCommandHandler;
        this.objectMapper = objectMapper;
    }

    @Override
    public void configure() {

        errorHandler(deadLetterChannel(
                        "kafka:{{app.kafka.topics.dlq}}?brokers={{app.kafka.bootstrap-servers}}")
                        .maximumRedeliveries(3)
                        .redeliveryDelay(1000)
                        .useExponentialBackOff()
                        .backOffMultiplier(2)
                        .maximumRedeliveryDelay(10000)
                        .logExhausted(true)
                        .logStackTrace(true)
                        .onPrepareFailure(exchange -> {
                            Throwable cause = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class);
                            Object body = exchange.getIn().getBody();
                            if (body instanceof ProcessAbbreviatedTaxInvoicePdfCommand cmd) {
                                log.error("DLQ: notifying orchestrator of retry exhaustion for saga {} document {}",
                                        cmd.getSagaId(), cmd.getDocumentNumber());
                                sagaCommandHandler.publishOrchestrationFailure(
                                        cmd.getSagaId(), cmd.getCorrelationId(),
                                        cmd.getDocumentId(), cmd.getDocumentNumber(), cause);
                            } else if (body instanceof CompensateAbbreviatedTaxInvoicePdfCommand cmd) {
                                log.error("DLQ: notifying orchestrator of compensation retry exhaustion for saga {} document {}",
                                        cmd.getSagaId(), cmd.getDocumentId());
                                sagaCommandHandler.publishCompensationOrchestrationFailure(
                                        cmd.getSagaId(), cmd.getCorrelationId(), cmd.getDocumentId(), cause);
                            } else {
                                log.error("DLQ: body not deserialized ({}); attempting saga metadata recovery",
                                        body == null ? "null" : body.getClass().getSimpleName());
                                recoverAndNotifyOrchestrator(body, cause);
                            }
                        }));

        from("kafka:{{app.kafka.topics.saga-command-abbreviated-tax-invoice-pdf}}"
                        + "?brokers={{app.kafka.bootstrap-servers}}"
                        + "&groupId={{app.kafka.consumer.command-group-id}}"
                        + "&autoOffsetReset=earliest"
                        + "&autoCommitEnable=false"
                        + "&breakOnFirstError={{app.kafka.consumer.break-on-first-error:true}}"
                        + "&maxPollRecords={{app.kafka.consumer.max-poll-records:100}}"
                        + "&consumersCount={{app.kafka.consumer.consumers-count:3}}")
                .routeId("saga-command-consumer")
                .log(LoggingLevel.DEBUG, "Received saga command from Kafka: partition=${header[kafka.PARTITION]}, offset=${header[kafka.OFFSET]}")
                .unmarshal().json(JsonLibrary.Jackson, ProcessAbbreviatedTaxInvoicePdfCommand.class)
                .process(exchange -> {
                    ProcessAbbreviatedTaxInvoicePdfCommand cmd =
                            exchange.getIn().getBody(ProcessAbbreviatedTaxInvoicePdfCommand.class);
                    log.info("Processing saga command for saga: {}, document: {}",
                            cmd.getSagaId(), cmd.getDocumentNumber());
                    processUseCase.handle(
                            cmd.getDocumentId(),
                            cmd.getDocumentNumber(),
                            cmd.getSignedXmlUrl(),
                            cmd.getSagaId(),
                            cmd.getSagaStep(),
                            cmd.getCorrelationId());
                })
                .log("Successfully processed saga command");

        from("kafka:{{app.kafka.topics.saga-compensation-abbreviated-tax-invoice-pdf}}"
                        + "?brokers={{app.kafka.bootstrap-servers}}"
                        + "&groupId={{app.kafka.consumer.compensation-group-id}}"
                        + "&autoOffsetReset=earliest"
                        + "&autoCommitEnable=false"
                        + "&breakOnFirstError={{app.kafka.consumer.break-on-first-error:true}}"
                        + "&maxPollRecords={{app.kafka.consumer.max-poll-records:100}}"
                        + "&consumersCount={{app.kafka.consumer.consumers-count:3}}")
                .routeId("saga-compensation-consumer")
                .log(LoggingLevel.DEBUG, "Received compensation command from Kafka: partition=${header[kafka.PARTITION]}, offset=${header[kafka.OFFSET]}")
                .unmarshal().json(JsonLibrary.Jackson, CompensateAbbreviatedTaxInvoicePdfCommand.class)
                .process(exchange -> {
                    CompensateAbbreviatedTaxInvoicePdfCommand cmd =
                            exchange.getIn().getBody(CompensateAbbreviatedTaxInvoicePdfCommand.class);
                    log.info("Processing compensation for saga: {}, document: {}",
                            cmd.getSagaId(), cmd.getDocumentId());
                    compensateUseCase.handle(
                            cmd.getDocumentId(),
                            cmd.getSagaId(),
                            cmd.getSagaStep(),
                            cmd.getCorrelationId());
                })
                .log("Successfully processed compensation command");
    }

    private void recoverAndNotifyOrchestrator(Object body, Throwable cause) {
        if (body == null) {
            log.error("DLQ: null message body — orchestrator must timeout");
            return;
        }
        try {
            byte[] rawBytes = body instanceof byte[] b
                    ? b : body.toString().getBytes(StandardCharsets.UTF_8);
            JsonNode node = objectMapper.readTree(rawBytes);
            String sagaId = node.path("sagaId").asText(null);
            String sagaStepStr = node.path("sajaStep").asText(null);
            String correlationId = node.path("correlationId").asText(null);
            if (sagaId == null || sagaStepStr == null) {
                log.error("DLQ: saga metadata missing in raw message — orchestrator must timeout");
                return;
            }
            SagaStep sagaStep = objectMapper.readValue("\"" + sagaStepStr + "\"", SagaStep.class);
            sagaCommandHandler.publishOrchestrationFailureForUnparsedMessage(sagaId, sagaStep, correlationId, cause);
        } catch (Exception e) {
            log.error("DLQ: cannot parse raw message for saga metadata — orchestrator must timeout", e);
        }
    }
}