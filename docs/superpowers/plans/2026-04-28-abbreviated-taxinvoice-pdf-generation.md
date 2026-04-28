# Abbreviated Tax Invoice PDF Generation Service — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Spring Boot microservice that generates PDF/A-3 documents for `AbbreviatedTaxInvoice_CrossIndustryInvoice` XML, uploads them to MinIO, and participates in the Saga Orchestration pipeline.

**Architecture:** Hexagonal port/adapter pattern, identical structure to `taxinvoice-pdf-generation-service` (port 8089). Key differences: AbbreviatedTaxInvoice XML namespaces, seller-only XSL template (no buyer party), port 8096, database `abbreviatedtaxinvoicepdf_db`, MinIO bucket `abbreviatedtaxinvoices`. Also adds `GENERATE_ABBREVIATED_TAX_INVOICE_PDF` to the shared `saga-commons` `SagaStep` enum.

**Tech Stack:** Java 21, Spring Boot 3.2.5, Apache Camel 4.14.4, Apache FOP 2.9, PDFBox 3.0.1, AWS SDK v2 S3 (MinIO), PostgreSQL + Flyway, Resilience4j, Micrometer/OpenTelemetry.

**Reference service:** All code below is adapted from `invoice-microservices/services/taxinvoice-pdf-generation-service/`. When in doubt, diff against that service.

**Working directory for all tasks in this service:** `invoice-microservices/services/abbreviatedtaxinvoice-pdf-generation-service/`

---

## File Map

### saga-commons (modified)
- Modify: `saga-commons/src/main/java/com/wpanther/saga/domain/enums/SagaStep.java`

### New service files

**Build + bootstrap**
- Create: `pom.xml`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/AbbreviatedTaxInvoicePdfGenerationServiceApplication.java`

**Domain**
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/domain/model/GenerationStatus.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/domain/model/AbbreviatedTaxInvoicePdfDocument.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/domain/repository/AbbreviatedTaxInvoicePdfDocumentRepository.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/domain/service/AbbreviatedTaxInvoicePdfGenerationService.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/domain/exception/AbbreviatedTaxInvoicePdfGenerationException.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/domain/constants/PdfGenerationConstants.java`

**Application ports**
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/application/port/out/PdfEventPort.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/application/port/out/PdfStoragePort.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/application/port/out/SagaReplyPort.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/application/port/out/SignedXmlFetchPort.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/application/usecase/ProcessAbbreviatedTaxInvoicePdfUseCase.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/application/usecase/CompensateAbbreviatedTaxInvoicePdfUseCase.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/application/service/SagaCommandHandler.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/application/service/AbbreviatedTaxInvoicePdfDocumentService.java`

**Infrastructure — Kafka (inbound)**
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/in/kafka/KafkaAbbreviatedTaxInvoiceProcessCommand.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/in/kafka/KafkaAbbreviatedTaxInvoiceCompensateCommand.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/in/kafka/KafkaCommandMapper.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/in/kafka/SagaRouteConfig.java`

**Infrastructure — messaging (outbound)**
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/messaging/AbbreviatedTaxInvoicePdfGeneratedEvent.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/messaging/AbbreviatedTaxInvoicePdfReplyEvent.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/messaging/OutboxConstants.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/messaging/EventPublisher.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/messaging/SagaReplyPublisher.java`

**Infrastructure — PDF generation**
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/pdf/FopAbbreviatedTaxInvoicePdfGenerator.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/pdf/PdfA3Converter.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/pdf/ThaiAmountWordsConverter.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/pdf/AbbreviatedTaxInvoicePdfGenerationServiceImpl.java`

**Infrastructure — persistence**
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/persistence/AbbreviatedTaxInvoicePdfDocumentEntity.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/persistence/JpaAbbreviatedTaxInvoicePdfDocumentRepository.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/persistence/AbbreviatedTaxInvoicePdfDocumentRepositoryAdapter.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/persistence/outbox/OutboxEventEntity.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/persistence/outbox/SpringDataOutboxRepository.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/persistence/outbox/JpaOutboxEventRepository.java`

**Infrastructure — storage**
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/storage/MinioStorageAdapter.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/storage/MinioCleanupService.java`

**Infrastructure — REST client**
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/client/RestTemplateSignedXmlFetcher.java`

**Infrastructure — config + metrics**
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/config/MinioConfig.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/config/OutboxConfig.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/config/RestTemplateConfig.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/config/FontHealthCheck.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/metrics/PdfGenerationMetrics.java`

**Resources**
- Create: `src/main/resources/application.yml`
- Create: `src/main/resources/db/migration/V1__create_abbreviated_tax_invoice_pdf_tables.sql`
- Create: `src/main/resources/xsl/abbreviatedtaxinvoice-direct.xsl`
- Copy: `src/main/resources/fop/fop.xconf` (from taxinvoice-pdf-generation-service)
- Copy: `src/main/resources/icc/sRGB.icc` (from taxinvoice-pdf-generation-service)
- Copy: `src/main/resources/fonts/` — all 6 TTF files (from taxinvoice-pdf-generation-service)

**Test resources + tests**
- Create: `src/test/resources/application-test.yml`
- Create: `src/test/resources/fop/fop.xconf`
- Create: `src/test/resources/xml/preview-abbreviatedtaxinvoice.xml`
- Create: (one test file per task — listed inline below)

---

## Task 1: Saga Commons — Add `GENERATE_ABBREVIATED_TAX_INVOICE_PDF`

**Files:**
- Modify: `saga-commons/src/main/java/com/wpanther/saga/domain/enums/SagaStep.java`

> **Working directory for this task:** `saga-commons/`

- [ ] **Step 1: Add the new enum constant**

Open `saga-commons/src/main/java/com/wpanther/saga/domain/enums/SagaStep.java`. Add after `GENERATE_RECEIPT_PDF` (line ~73):

```java
/**
 * Abbreviated tax invoice PDF generation via abbreviatedtaxinvoice-pdf-generation-service.
 */
GENERATE_ABBREVIATED_TAX_INVOICE_PDF("generate-abbreviated-tax-invoice-pdf", "Abbreviated Tax Invoice PDF Generation Service"),
```

The block after the change should look like:

```java
GENERATE_RECEIPT_PDF("generate-receipt-pdf", "Receipt PDF Generation Service"),

/**
 * Abbreviated tax invoice PDF generation via abbreviatedtaxinvoice-pdf-generation-service.
 */
GENERATE_ABBREVIATED_TAX_INVOICE_PDF("generate-abbreviated-tax-invoice-pdf", "Abbreviated Tax Invoice PDF Generation Service"),

/**
 * PDF signing via pdf-signing-service.
 */
SIGN_PDF("sign-pdf", "PDF Signing Service"),
```

- [ ] **Step 2: Verify it compiles and existing tests still pass**

```bash
cd saga-commons
mvn test
```

Expected: `BUILD SUCCESS`. All existing `SagaStep` tests pass.

- [ ] **Step 3: Install to local Maven repo**

```bash
mvn clean install -DskipTests
```

Expected: `BUILD SUCCESS`, artifact `com.wpanther:saga-commons:1.0.0-SNAPSHOT` installed.

- [ ] **Step 4: Commit**

```bash
cd saga-commons
git add src/main/java/com/wpanther/saga/domain/enums/SagaStep.java
git commit -m "feat: add GENERATE_ABBREVIATED_TAX_INVOICE_PDF saga step"
```

---

## Task 2: Maven Project Scaffold — `pom.xml` + Main Application Class

**Files:**
- Create: `pom.xml`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/AbbreviatedTaxInvoicePdfGenerationServiceApplication.java`

> **Working directory for all remaining tasks:** `invoice-microservices/services/abbreviatedtaxinvoice-pdf-generation-service/`

- [ ] **Step 1: Create `pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.wpanther</groupId>
    <artifactId>abbreviatedtaxinvoice-pdf-generation-service</artifactId>
    <version>1.0.0-SNAPSHOT</version>
    <packaging>jar</packaging>

    <name>Abbreviated Tax Invoice PDF Generation Service</name>
    <description>Microservice for generating PDF/A-3 documents for Thai e-Tax abbreviated tax invoices with embedded XML</description>

    <properties>
        <java.version>21</java.version>
        <maven.compiler.source>21</maven.compiler.source>
        <maven.compiler.target>21</maven.compiler.target>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>

        <spring-boot.version>3.2.5</spring-boot.version>
        <spring-cloud.version>2023.0.1</spring-cloud.version>
        <fop.version>2.9</fop.version>
        <pdfbox.version>3.0.1</pdfbox.version>
        <lombok.version>1.18.30</lombok.version>
        <flyway.version>10.10.0</flyway.version>
        <camel.version>4.14.4</camel.version>
        <saga.commons.version>1.0.0-SNAPSHOT</saga.commons.version>
        <aws-sdk.version>2.20.26</aws-sdk.version>
        <resilience4j.version>2.1.0</resilience4j.version>
    </properties>

    <dependencyManagement>
        <dependencies>
            <dependency>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-dependencies</artifactId>
                <version>${spring-boot.version}</version>
                <type>pom</type>
                <scope>import</scope>
            </dependency>
            <dependency>
                <groupId>org.springframework.cloud</groupId>
                <artifactId>spring-cloud-dependencies</artifactId>
                <version>${spring-cloud.version}</version>
                <type>pom</type>
                <scope>import</scope>
            </dependency>
            <dependency>
                <groupId>org.apache.camel.springboot</groupId>
                <artifactId>camel-spring-boot-bom</artifactId>
                <version>${camel.version}</version>
                <type>pom</type>
                <scope>import</scope>
            </dependency>
            <dependency>
                <groupId>software.amazon.awssdk</groupId>
                <artifactId>bom</artifactId>
                <version>${aws-sdk.version}</version>
                <type>pom</type>
                <scope>import</scope>
            </dependency>
        </dependencies>
    </dependencyManagement>

    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-data-jpa</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-validation</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-actuator</artifactId>
        </dependency>
        <dependency>
            <groupId>org.apache.camel.springboot</groupId>
            <artifactId>camel-spring-boot-starter</artifactId>
        </dependency>
        <dependency>
            <groupId>org.apache.camel.springboot</groupId>
            <artifactId>camel-kafka-starter</artifactId>
        </dependency>
        <dependency>
            <groupId>org.apache.camel.springboot</groupId>
            <artifactId>camel-jackson-starter</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.cloud</groupId>
            <artifactId>spring-cloud-starter-netflix-eureka-client</artifactId>
        </dependency>
        <dependency>
            <groupId>org.postgresql</groupId>
            <artifactId>postgresql</artifactId>
            <scope>runtime</scope>
        </dependency>
        <dependency>
            <groupId>org.flywaydb</groupId>
            <artifactId>flyway-core</artifactId>
            <version>${flyway.version}</version>
        </dependency>
        <dependency>
            <groupId>org.flywaydb</groupId>
            <artifactId>flyway-database-postgresql</artifactId>
            <version>${flyway.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.xmlgraphics</groupId>
            <artifactId>fop</artifactId>
            <version>${fop.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.pdfbox</groupId>
            <artifactId>pdfbox</artifactId>
            <version>${pdfbox.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.pdfbox</groupId>
            <artifactId>xmpbox</artifactId>
            <version>${pdfbox.version}</version>
        </dependency>
        <dependency>
            <groupId>jakarta.xml.bind</groupId>
            <artifactId>jakarta.xml.bind-api</artifactId>
        </dependency>
        <dependency>
            <groupId>org.glassfish.jaxb</groupId>
            <artifactId>jaxb-runtime</artifactId>
        </dependency>
        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
            <version>${lombok.version}</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
        </dependency>
        <dependency>
            <groupId>com.fasterxml.jackson.datatype</groupId>
            <artifactId>jackson-datatype-jsr310</artifactId>
        </dependency>
        <dependency>
            <groupId>com.wpanther</groupId>
            <artifactId>saga-commons</artifactId>
            <version>${saga.commons.version}</version>
        </dependency>
        <dependency>
            <groupId>software.amazon.awssdk</groupId>
            <artifactId>s3</artifactId>
        </dependency>
        <dependency>
            <groupId>io.github.resilience4j</groupId>
            <artifactId>resilience4j-spring-boot3</artifactId>
            <version>${resilience4j.version}</version>
        </dependency>
        <dependency>
            <groupId>io.micrometer</groupId>
            <artifactId>micrometer-registry-prometheus</artifactId>
        </dependency>
        <dependency>
            <groupId>io.micrometer</groupId>
            <artifactId>micrometer-tracing-bridge-otel</artifactId>
        </dependency>
        <dependency>
            <groupId>io.opentelemetry</groupId>
            <artifactId>opentelemetry-exporter-otlp</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.camel</groupId>
            <artifactId>camel-test-spring-junit5</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>com.h2database</groupId>
            <artifactId>h2</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
                <version>${spring-boot.version}</version>
                <configuration>
                    <excludes>
                        <exclude>
                            <groupId>org.projectlombok</groupId>
                            <artifactId>lombok</artifactId>
                        </exclude>
                    </excludes>
                </configuration>
                <executions>
                    <execution>
                        <goals>
                            <goal>repackage</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.13.0</version>
                <configuration>
                    <source>21</source>
                    <target>21</target>
                    <annotationProcessorPaths>
                        <path>
                            <groupId>org.projectlombok</groupId>
                            <artifactId>lombok</artifactId>
                            <version>${lombok.version}</version>
                        </path>
                    </annotationProcessorPaths>
                </configuration>
            </plugin>
            <plugin>
                <groupId>org.flywaydb</groupId>
                <artifactId>flyway-maven-plugin</artifactId>
                <version>${flyway.version}</version>
            </plugin>
        </plugins>
    </build>
</project>
```

- [ ] **Step 2: Create main application class**

Create `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/AbbreviatedTaxInvoicePdfGenerationServiceApplication.java`:

```java
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
```

- [ ] **Step 3: Verify the project compiles with no sources (dependency resolution only)**

```bash
mvn compile -DskipTests 2>&1 | tail -5
```

Expected: `BUILD SUCCESS` (no Java sources yet — Maven just resolves dependencies).

- [ ] **Step 4: Commit**

```bash
git add pom.xml src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/AbbreviatedTaxInvoicePdfGenerationServiceApplication.java
git commit -m "feat: scaffold pom.xml and main application class"
```

---

## Task 3: Domain Layer — Exception, Constants, `GenerationStatus`, and Repository/Service interfaces

**Files:**
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/domain/exception/AbbreviatedTaxInvoicePdfGenerationException.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/domain/constants/PdfGenerationConstants.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/domain/model/GenerationStatus.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/domain/repository/AbbreviatedTaxInvoicePdfDocumentRepository.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/domain/service/AbbreviatedTaxInvoicePdfGenerationService.java`
- Test: `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/domain/exception/AbbreviatedTaxInvoicePdfGenerationExceptionTest.java`
- Test: `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/domain/constants/PdfGenerationConstantsTest.java`

- [ ] **Step 1: Write the failing tests first**

Create `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/domain/exception/AbbreviatedTaxInvoicePdfGenerationExceptionTest.java`:

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.domain.exception;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class AbbreviatedTaxInvoicePdfGenerationExceptionTest {

    @Test
    void shouldCreateWithMessage() {
        var ex = new AbbreviatedTaxInvoicePdfGenerationException("test error");
        assertThat(ex.getMessage()).isEqualTo("test error");
        assertThat(ex).isInstanceOf(RuntimeException.class);
    }

    @Test
    void shouldCreateWithMessageAndCause() {
        var cause = new IllegalStateException("root cause");
        var ex = new AbbreviatedTaxInvoicePdfGenerationException("wrapped", cause);
        assertThat(ex.getMessage()).isEqualTo("wrapped");
        assertThat(ex.getCause()).isSameAs(cause);
    }
}
```

Create `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/domain/constants/PdfGenerationConstantsTest.java`:

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.domain.constants;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class PdfGenerationConstantsTest {

    @Test
    void shouldHaveExpectedDefaultMaxRetries() {
        assertThat(PdfGenerationConstants.DEFAULT_MAX_RETRIES).isEqualTo(3);
    }

    @Test
    void shouldHaveExpectedPdfMimeType() {
        assertThat(PdfGenerationConstants.PDF_MIME_TYPE).isEqualTo("application/pdf");
    }
}
```

- [ ] **Step 2: Create `src/test/resources/application-test.yml`** (needed for test context later; do it now)

```yaml
spring:
  datasource:
    url: jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE
    driver-class-name: org.h2.Driver
    username: sa
    password:
  jpa:
    database-platform: org.hibernate.dialect.H2Dialect
    hibernate:
      ddl-auto: create-drop
    show-sql: false
  flyway:
    enabled: false

app:
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      command-group-id: test-command-group
      compensation-group-id: test-compensation-group
      break-on-first-error: false
      max-poll-records: 10
      consumers-count: 1
    topics:
      saga-command-abbreviated-tax-invoice-pdf: saga.command.abbreviated-tax-invoice-pdf
      saga-compensation-abbreviated-tax-invoice-pdf: saga.compensation.abbreviated-tax-invoice-pdf
      pdf-generated-abbreviated-tax-invoice: pdf.generated.abbreviated-tax-invoice
      dlq: pdf.generation.abbreviated-tax-invoice.dlq
  minio:
    endpoint: http://localhost:9000
    access-key: minioadmin
    secret-key: minioadmin
    bucket-name: abbreviatedtaxinvoices
    region: us-east-1
    base-url: http://localhost:9000/abbreviatedtaxinvoices
    path-style-access: true
    cleanup:
      enabled: false
      cron: "0 0 2 * * ?"
  pdf:
    icc-profile-path: icc/sRGB.icc
    generation:
      max-retries: 3
      max-concurrent-renders: 3
      max-pdf-size-bytes: 52428800
  abbreviatedtaxinvoice:
    default-vat-rate: 7
  rest-client:
    connect-timeout: 5000
    read-timeout: 10000
    allowed-hosts: localhost
  fonts:
    health-check:
      enabled: false
      fail-on-error: false

eureka:
  client:
    enabled: false

resilience4j:
  circuitbreaker:
    instances:
      minio:
        sliding-window-size: 10
        failure-rate-threshold: 50
        wait-duration-in-open-state: 30s
        permitted-number-of-calls-in-half-open-state: 3
        automatic-transition-from-open-to-half-open-enabled: true
      signedXmlFetch:
        sliding-window-size: 10
        failure-rate-threshold: 50
        wait-duration-in-open-state: 60s
        permitted-number-of-calls-in-half-open-state: 3
        slow-call-rate-threshold: 50
        slow-call-duration-threshold: 3s
```

- [ ] **Step 3: Run the failing tests**

```bash
mvn test -Dtest="AbbreviatedTaxInvoicePdfGenerationExceptionTest,PdfGenerationConstantsTest" -Dspring.profiles.active=test 2>&1 | tail -20
```

Expected: compilation error — `AbbreviatedTaxInvoicePdfGenerationException` and `PdfGenerationConstants` do not exist yet.

- [ ] **Step 4: Create the domain support classes**

Create `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/domain/exception/AbbreviatedTaxInvoicePdfGenerationException.java`:

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.domain.exception;

public class AbbreviatedTaxInvoicePdfGenerationException extends RuntimeException {

    public AbbreviatedTaxInvoicePdfGenerationException(String message) {
        super(message);
    }

    public AbbreviatedTaxInvoicePdfGenerationException(String message, Throwable cause) {
        super(message, cause);
    }
}
```

Create `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/domain/constants/PdfGenerationConstants.java`:

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.domain.constants;

public final class PdfGenerationConstants {

    private PdfGenerationConstants() {}

    public static final int DEFAULT_MAX_RETRIES = 3;
    public static final String PDF_MIME_TYPE = "application/pdf";
}
```

Create `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/domain/model/GenerationStatus.java`:

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.domain.model;

public enum GenerationStatus {
    PENDING,
    GENERATING,
    COMPLETED,
    FAILED
}
```

Create `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/domain/repository/AbbreviatedTaxInvoicePdfDocumentRepository.java`:

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.domain.repository;

import com.wpanther.abbreviatedtaxinvoice.pdf.domain.model.AbbreviatedTaxInvoicePdfDocument;
import java.util.Optional;
import java.util.UUID;

public interface AbbreviatedTaxInvoicePdfDocumentRepository {

    AbbreviatedTaxInvoicePdfDocument save(AbbreviatedTaxInvoicePdfDocument document);

    Optional<AbbreviatedTaxInvoicePdfDocument> findById(UUID id);

    Optional<AbbreviatedTaxInvoicePdfDocument> findByAbbreviatedTaxInvoiceId(String abbreviatedTaxInvoiceId);

    void deleteById(UUID id);

    void flush();
}
```

Create `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/domain/service/AbbreviatedTaxInvoicePdfGenerationService.java`:

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.domain.service;

public interface AbbreviatedTaxInvoicePdfGenerationService {

    /**
     * Generate PDF/A-3 from the signed XML document.
     *
     * @param abbreviatedTaxInvoiceNumber document number (used for logging and file naming)
     * @param signedXml full Thai e-Tax signed XML (rsm:AbbreviatedTaxInvoice_CrossIndustryInvoice)
     * @return PDF/A-3 bytes with the signed XML embedded as an attachment
     * @throws AbbreviatedTaxInvoicePdfGenerationException if generation fails
     */
    byte[] generatePdf(String abbreviatedTaxInvoiceNumber, String signedXml)
        throws AbbreviatedTaxInvoicePdfGenerationException;

    class AbbreviatedTaxInvoicePdfGenerationException extends Exception {
        public AbbreviatedTaxInvoicePdfGenerationException(String message) { super(message); }
        public AbbreviatedTaxInvoicePdfGenerationException(String message, Throwable cause) { super(message, cause); }
    }
}
```

- [ ] **Step 5: Run the tests to verify they pass**

```bash
mvn test -Dtest="AbbreviatedTaxInvoicePdfGenerationExceptionTest,PdfGenerationConstantsTest" 2>&1 | tail -10
```

Expected: `BUILD SUCCESS`, 4 tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/
git commit -m "feat: add domain support types (exception, constants, status, repository/service interfaces)"
```

---

## Task 4: Domain Model — `AbbreviatedTaxInvoicePdfDocument` Aggregate Root

**Files:**
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/domain/model/AbbreviatedTaxInvoicePdfDocument.java`
- Test: `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/domain/model/AbbreviatedTaxInvoicePdfDocumentTest.java`

- [ ] **Step 1: Write the failing test**

Create `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/domain/model/AbbreviatedTaxInvoicePdfDocumentTest.java`:

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.domain.model;

import com.wpanther.abbreviatedtaxinvoice.pdf.domain.exception.AbbreviatedTaxInvoicePdfGenerationException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

@DisplayName("AbbreviatedTaxInvoicePdfDocument Aggregate Tests")
class AbbreviatedTaxInvoicePdfDocumentTest {

    private AbbreviatedTaxInvoicePdfDocument pendingDocument() {
        return AbbreviatedTaxInvoicePdfDocument.builder()
                .abbreviatedTaxInvoiceId("ati-001")
                .abbreviatedTaxInvoiceNumber("423-612")
                .build();
    }

    @Test
    @DisplayName("Should create document in PENDING status with defaults")
    void testCreate_Defaults() {
        AbbreviatedTaxInvoicePdfDocument doc = pendingDocument();

        assertThat(doc.getId()).isNotNull();
        assertThat(doc.getStatus()).isEqualTo(GenerationStatus.PENDING);
        assertThat(doc.getMimeType()).isEqualTo("application/pdf");
        assertThat(doc.getRetryCount()).isZero();
        assertThat(doc.isXmlEmbedded()).isFalse();
        assertThat(doc.getCreatedAt()).isNotNull();
    }

    @Test
    @DisplayName("Should reject blank abbreviatedTaxInvoiceId")
    void testCreate_BlankId() {
        assertThatThrownBy(() ->
                AbbreviatedTaxInvoicePdfDocument.builder()
                        .abbreviatedTaxInvoiceId("   ")
                        .abbreviatedTaxInvoiceNumber("423-612")
                        .build()
        ).isInstanceOf(AbbreviatedTaxInvoicePdfGenerationException.class)
         .hasMessageContaining("Abbreviated Tax Invoice ID cannot be blank");
    }

    @Test
    @DisplayName("Should reject null abbreviatedTaxInvoiceNumber")
    void testCreate_NullNumber() {
        assertThatThrownBy(() ->
                AbbreviatedTaxInvoicePdfDocument.builder()
                        .abbreviatedTaxInvoiceId("ati-001")
                        .abbreviatedTaxInvoiceNumber(null)
                        .build()
        ).isInstanceOf(NullPointerException.class);
    }

    @Test
    @DisplayName("PENDING → startGeneration() → GENERATING")
    void testStartGeneration() {
        AbbreviatedTaxInvoicePdfDocument doc = pendingDocument();
        doc.startGeneration();
        assertThat(doc.getStatus()).isEqualTo(GenerationStatus.GENERATING);
    }

    @Test
    @DisplayName("GENERATING → markCompleted() → COMPLETED")
    void testMarkCompleted() {
        AbbreviatedTaxInvoicePdfDocument doc = pendingDocument();
        doc.startGeneration();
        doc.markCompleted("2026/04/28/test.pdf", "http://minio/test.pdf", 98765L);

        assertThat(doc.getStatus()).isEqualTo(GenerationStatus.COMPLETED);
        assertThat(doc.getDocumentPath()).isEqualTo("2026/04/28/test.pdf");
        assertThat(doc.getDocumentUrl()).isEqualTo("http://minio/test.pdf");
        assertThat(doc.getFileSize()).isEqualTo(98765L);
        assertThat(doc.getCompletedAt()).isNotNull();
        assertThat(doc.isCompleted()).isTrue();
        assertThat(doc.isSuccessful()).isTrue();
    }

    @Test
    @DisplayName("Any state → markFailed() → FAILED")
    void testMarkFailed_FromPending() {
        AbbreviatedTaxInvoicePdfDocument doc = pendingDocument();
        doc.markFailed("Something went wrong");

        assertThat(doc.getStatus()).isEqualTo(GenerationStatus.FAILED);
        assertThat(doc.getErrorMessage()).isEqualTo("Something went wrong");
        assertThat(doc.isFailed()).isTrue();
        assertThat(doc.isCompleted()).isFalse();
        assertThat(doc.getCompletedAt()).isNotNull();
    }

    @Test
    @DisplayName("markXmlEmbedded() sets flag")
    void testMarkXmlEmbedded() {
        AbbreviatedTaxInvoicePdfDocument doc = pendingDocument();
        assertThat(doc.isXmlEmbedded()).isFalse();
        doc.markXmlEmbedded();
        assertThat(doc.isXmlEmbedded()).isTrue();
    }

    @Test
    @DisplayName("startGeneration() from GENERATING throws exception")
    void testStartGeneration_AlreadyGenerating() {
        AbbreviatedTaxInvoicePdfDocument doc = pendingDocument();
        doc.startGeneration();

        assertThatThrownBy(doc::startGeneration)
                .isInstanceOf(AbbreviatedTaxInvoicePdfGenerationException.class)
                .hasMessageContaining("PENDING");
    }

    @Test
    @DisplayName("markCompleted() from PENDING throws exception")
    void testMarkCompleted_FromPending() {
        AbbreviatedTaxInvoicePdfDocument doc = pendingDocument();

        assertThatThrownBy(() -> doc.markCompleted("path", "url", 100L))
                .isInstanceOf(AbbreviatedTaxInvoicePdfGenerationException.class)
                .hasMessageContaining("GENERATING");
    }

    @Test
    @DisplayName("markCompleted() with zero fileSize throws IllegalArgumentException")
    void testMarkCompleted_ZeroFileSize() {
        AbbreviatedTaxInvoicePdfDocument doc = pendingDocument();
        doc.startGeneration();

        assertThatThrownBy(() -> doc.markCompleted("path", "url", 0L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("File size must be positive");
    }

    @Test
    @DisplayName("incrementRetryCount() increases count by one")
    void testIncrementRetryCount() {
        AbbreviatedTaxInvoicePdfDocument doc = pendingDocument();
        assertThat(doc.getRetryCount()).isZero();
        doc.incrementRetryCount();
        assertThat(doc.getRetryCount()).isOne();
    }

    @Test
    @DisplayName("incrementRetryCountTo() advances count monotonically")
    void testIncrementRetryCountTo() {
        AbbreviatedTaxInvoicePdfDocument doc = pendingDocument();
        doc.incrementRetryCountTo(2);
        assertThat(doc.getRetryCount()).isEqualTo(2);

        doc.incrementRetryCountTo(1); // no-op: current is higher
        assertThat(doc.getRetryCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("isMaxRetriesExceeded() returns true when retryCount >= maxRetries")
    void testIsMaxRetriesExceeded() {
        AbbreviatedTaxInvoicePdfDocument doc = AbbreviatedTaxInvoicePdfDocument.builder()
                .abbreviatedTaxInvoiceId("ati-001")
                .abbreviatedTaxInvoiceNumber("423-612")
                .retryCount(3)
                .build();
        assertThat(doc.isMaxRetriesExceeded(3)).isTrue();
        assertThat(doc.isMaxRetriesExceeded(4)).isFalse();
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
mvn test -Dtest="AbbreviatedTaxInvoicePdfDocumentTest" 2>&1 | tail -10
```

Expected: compilation error — `AbbreviatedTaxInvoicePdfDocument` does not exist.

- [ ] **Step 3: Create `AbbreviatedTaxInvoicePdfDocument`**

Create `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/domain/model/AbbreviatedTaxInvoicePdfDocument.java`:

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.domain.model;

import com.wpanther.abbreviatedtaxinvoice.pdf.domain.exception.AbbreviatedTaxInvoicePdfGenerationException;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.UUID;

public class AbbreviatedTaxInvoicePdfDocument {

    private static final String DEFAULT_MIME_TYPE = "application/pdf";

    private final UUID id;
    private final String abbreviatedTaxInvoiceId;
    private final String abbreviatedTaxInvoiceNumber;
    private String documentPath;
    private String documentUrl;
    private long fileSize;
    private final String mimeType;
    private boolean xmlEmbedded;
    private GenerationStatus status;
    private String errorMessage;
    private int retryCount;
    private final LocalDateTime createdAt;
    private LocalDateTime completedAt;

    private AbbreviatedTaxInvoicePdfDocument(Builder builder) {
        this.id = builder.id != null ? builder.id : UUID.randomUUID();
        this.abbreviatedTaxInvoiceId = Objects.requireNonNull(builder.abbreviatedTaxInvoiceId, "Abbreviated Tax Invoice ID is required");
        this.abbreviatedTaxInvoiceNumber = Objects.requireNonNull(builder.abbreviatedTaxInvoiceNumber, "Abbreviated Tax Invoice number is required");
        this.documentPath = builder.documentPath;
        this.documentUrl = builder.documentUrl;
        this.fileSize = builder.fileSize;
        this.mimeType = builder.mimeType != null ? builder.mimeType : DEFAULT_MIME_TYPE;
        this.xmlEmbedded = builder.xmlEmbedded;
        this.status = builder.status != null ? builder.status : GenerationStatus.PENDING;
        this.errorMessage = builder.errorMessage;
        this.retryCount = builder.retryCount;
        this.createdAt = builder.createdAt != null ? builder.createdAt : LocalDateTime.now();
        this.completedAt = builder.completedAt;
        validateInvariant();
    }

    private void validateInvariant() {
        if (abbreviatedTaxInvoiceId.isBlank()) {
            throw new AbbreviatedTaxInvoicePdfGenerationException("Abbreviated Tax Invoice ID cannot be blank");
        }
        if (abbreviatedTaxInvoiceNumber.isBlank()) {
            throw new AbbreviatedTaxInvoicePdfGenerationException("Abbreviated Tax Invoice number cannot be blank");
        }
    }

    public void startGeneration() {
        if (this.status != GenerationStatus.PENDING) {
            throw new AbbreviatedTaxInvoicePdfGenerationException("Can only start generation from PENDING status");
        }
        this.status = GenerationStatus.GENERATING;
    }

    public void markCompleted(String documentPath, String documentUrl, long fileSize) {
        if (this.status != GenerationStatus.GENERATING) {
            throw new AbbreviatedTaxInvoicePdfGenerationException("Can only complete from GENERATING status");
        }
        Objects.requireNonNull(documentPath, "Document path is required");
        Objects.requireNonNull(documentUrl, "Document URL is required");
        if (fileSize <= 0) {
            throw new IllegalArgumentException("File size must be positive");
        }
        this.documentPath = documentPath;
        this.documentUrl = documentUrl;
        this.fileSize = fileSize;
        this.status = GenerationStatus.COMPLETED;
        this.completedAt = LocalDateTime.now();
    }

    public void markFailed(String errorMessage) {
        this.status = GenerationStatus.FAILED;
        this.errorMessage = errorMessage;
        this.completedAt = LocalDateTime.now();
    }

    public void markXmlEmbedded() {
        this.xmlEmbedded = true;
    }

    public boolean isSuccessful() { return status == GenerationStatus.COMPLETED; }
    public boolean isCompleted()  { return status == GenerationStatus.COMPLETED; }
    public boolean isFailed()     { return status == GenerationStatus.FAILED; }

    public void incrementRetryCount() { this.retryCount++; }

    public void incrementRetryCountTo(int target) {
        if (target < 0) throw new IllegalArgumentException("Target retry count cannot be negative");
        if (this.retryCount < target) this.retryCount = target;
    }

    public void setRetryCount(int retryCount) {
        if (retryCount < 0) throw new IllegalArgumentException("Retry count cannot be negative");
        this.retryCount = retryCount;
    }

    public boolean isMaxRetriesExceeded(int maxRetries) { return this.retryCount >= maxRetries; }

    public UUID getId()                           { return id; }
    public String getAbbreviatedTaxInvoiceId()    { return abbreviatedTaxInvoiceId; }
    public String getAbbreviatedTaxInvoiceNumber(){ return abbreviatedTaxInvoiceNumber; }
    public String getDocumentPath()               { return documentPath; }
    public String getDocumentUrl()                { return documentUrl; }
    public long getFileSize()                     { return fileSize; }
    public String getMimeType()                   { return mimeType; }
    public boolean isXmlEmbedded()                { return xmlEmbedded; }
    public GenerationStatus getStatus()           { return status; }
    public String getErrorMessage()               { return errorMessage; }
    public LocalDateTime getCreatedAt()           { return createdAt; }
    public LocalDateTime getCompletedAt()         { return completedAt; }
    public int getRetryCount()                    { return retryCount; }

    public static Builder builder() { return new Builder(); }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AbbreviatedTaxInvoicePdfDocument other)) return false;
        return Objects.equals(id, other.id);
    }

    @Override
    public int hashCode() { return Objects.hashCode(id); }

    @Override
    public String toString() {
        return "AbbreviatedTaxInvoicePdfDocument{id=" + id
                + ", abbreviatedTaxInvoiceId='" + abbreviatedTaxInvoiceId + '\''
                + ", abbreviatedTaxInvoiceNumber='" + abbreviatedTaxInvoiceNumber + '\''
                + ", status=" + status
                + ", retryCount=" + retryCount + '}';
    }

    public static class Builder {
        private UUID id;
        private String abbreviatedTaxInvoiceId;
        private String abbreviatedTaxInvoiceNumber;
        private String documentPath;
        private String documentUrl;
        private long fileSize;
        private String mimeType;
        private boolean xmlEmbedded;
        private GenerationStatus status;
        private String errorMessage;
        private int retryCount;
        private LocalDateTime createdAt;
        private LocalDateTime completedAt;

        public Builder id(UUID id)                                              { this.id = id; return this; }
        public Builder abbreviatedTaxInvoiceId(String v)                        { this.abbreviatedTaxInvoiceId = v; return this; }
        public Builder abbreviatedTaxInvoiceNumber(String v)                    { this.abbreviatedTaxInvoiceNumber = v; return this; }
        public Builder documentPath(String v)                                   { this.documentPath = v; return this; }
        public Builder documentUrl(String v)                                    { this.documentUrl = v; return this; }
        public Builder fileSize(long v)                                         { this.fileSize = v; return this; }
        public Builder mimeType(String v)                                       { this.mimeType = v; return this; }
        public Builder xmlEmbedded(boolean v)                                   { this.xmlEmbedded = v; return this; }
        public Builder status(GenerationStatus v)                               { this.status = v; return this; }
        public Builder errorMessage(String v)                                   { this.errorMessage = v; return this; }
        public Builder retryCount(int v)                                        { this.retryCount = v; return this; }
        public Builder createdAt(LocalDateTime v)                               { this.createdAt = v; return this; }
        public Builder completedAt(LocalDateTime v)                             { this.completedAt = v; return this; }

        public AbbreviatedTaxInvoicePdfDocument build() {
            return new AbbreviatedTaxInvoicePdfDocument(this);
        }
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
mvn test -Dtest="AbbreviatedTaxInvoicePdfDocumentTest,AbbreviatedTaxInvoicePdfGenerationExceptionTest,PdfGenerationConstantsTest" 2>&1 | tail -10
```

Expected: `BUILD SUCCESS`, all 15 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/
git commit -m "feat: add AbbreviatedTaxInvoicePdfDocument aggregate root and domain layer"
```

---

## Task 5: Application Ports + Use Case Interfaces

**Files:**
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/application/port/out/PdfEventPort.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/application/port/out/PdfStoragePort.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/application/port/out/SagaReplyPort.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/application/port/out/SignedXmlFetchPort.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/application/usecase/ProcessAbbreviatedTaxInvoicePdfUseCase.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/application/usecase/CompensateAbbreviatedTaxInvoicePdfUseCase.java`

These are all pure interfaces — no tests required (the implementations are tested separately).

- [ ] **Step 1: Create `PdfEventPort`**

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.application.port.out;

import com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.messaging.AbbreviatedTaxInvoicePdfGeneratedEvent;

public interface PdfEventPort {
    void publishPdfGenerated(AbbreviatedTaxInvoicePdfGeneratedEvent event);
}
```

- [ ] **Step 2: Create `PdfStoragePort`**

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.application.port.out;

public interface PdfStoragePort {
    /** Upload PDF bytes and return the S3 key. */
    String store(String abbreviatedTaxInvoiceNumber, byte[] pdfBytes);
    /** Delete a stored PDF by S3 key (best-effort). */
    void delete(String s3Key);
    /** Resolve a full URL from an S3 key. */
    String resolveUrl(String s3Key);
}
```

- [ ] **Step 3: Create `SagaReplyPort`**

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.application.port.out;

import com.wpanther.saga.domain.enums.SagaStep;

public interface SagaReplyPort {
    void publishSuccess(String sagaId, SagaStep sagaStep, String correlationId,
                        String pdfUrl, long pdfSize);
    void publishFailure(String sagaId, SagaStep sagaStep, String correlationId,
                        String errorMessage);
    void publishCompensated(String sagaId, SagaStep sagaStep, String correlationId);
}
```

- [ ] **Step 4: Create `SignedXmlFetchPort`**

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.application.port.out;

public interface SignedXmlFetchPort {

    /**
     * Download the signed XML content from the given URL.
     *
     * @param url URL pointing to the signed XML document
     * @return non-blank XML content
     * @throws SignedXmlFetchException if the HTTP request fails or the response is blank
     */
    String fetch(String url);

    class SignedXmlFetchException extends RuntimeException {
        public SignedXmlFetchException(String message) { super(message); }
        public SignedXmlFetchException(String message, Throwable cause) { super(message, cause); }
    }
}
```

- [ ] **Step 5: Create use case interfaces**

Create `ProcessAbbreviatedTaxInvoicePdfUseCase.java`:

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.application.usecase;

import com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.in.kafka.KafkaAbbreviatedTaxInvoiceProcessCommand;

public interface ProcessAbbreviatedTaxInvoicePdfUseCase {
    void handle(KafkaAbbreviatedTaxInvoiceProcessCommand command);
}
```

Create `CompensateAbbreviatedTaxInvoicePdfUseCase.java`:

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.application.usecase;

import com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.in.kafka.KafkaAbbreviatedTaxInvoiceCompensateCommand;

public interface CompensateAbbreviatedTaxInvoicePdfUseCase {
    void handle(KafkaAbbreviatedTaxInvoiceCompensateCommand command);
}
```

- [ ] **Step 6: Verify the project still compiles**

```bash
mvn compile -DskipTests 2>&1 | tail -5
```

Expected: `BUILD SUCCESS`.

- [ ] **Step 7: Commit**

```bash
git add src/
git commit -m "feat: add application port interfaces and use case contracts"
```

---

## Task 6: Infrastructure Persistence — Entity, JPA Repository, Repository Adapter

**Files:**
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/persistence/AbbreviatedTaxInvoicePdfDocumentEntity.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/persistence/JpaAbbreviatedTaxInvoicePdfDocumentRepository.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/persistence/AbbreviatedTaxInvoicePdfDocumentRepositoryAdapter.java`
- Test: `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/persistence/JpaAbbreviatedTaxInvoicePdfDocumentRepositoryImplTest.java`

- [ ] **Step 1: Write the failing test**

Create `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/persistence/JpaAbbreviatedTaxInvoicePdfDocumentRepositoryImplTest.java`:

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.persistence;

import com.wpanther.abbreviatedtaxinvoice.pdf.domain.model.AbbreviatedTaxInvoicePdfDocument;
import com.wpanther.abbreviatedtaxinvoice.pdf.domain.model.GenerationStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("AbbreviatedTaxInvoicePdfDocumentRepositoryAdapter Unit Tests")
class JpaAbbreviatedTaxInvoicePdfDocumentRepositoryImplTest {

    @Mock
    private JpaAbbreviatedTaxInvoicePdfDocumentRepository jpaRepository;

    private AbbreviatedTaxInvoicePdfDocumentRepositoryAdapter repository;

    private UUID id;
    private AbbreviatedTaxInvoicePdfDocument domain;

    @BeforeEach
    void setUp() {
        repository = new AbbreviatedTaxInvoicePdfDocumentRepositoryAdapter(jpaRepository);
        id = UUID.randomUUID();
        LocalDateTime now = LocalDateTime.now();

        domain = AbbreviatedTaxInvoicePdfDocument.builder()
                .id(id)
                .abbreviatedTaxInvoiceId("ati-123")
                .abbreviatedTaxInvoiceNumber("423-612")
                .documentPath("2026/04/28/abbreviated-tax-invoice-423-612-abc.pdf")
                .documentUrl("http://localhost:9000/abbreviatedtaxinvoices/2026/04/28/abbreviated-tax-invoice-423-612-abc.pdf")
                .fileSize(98765L)
                .mimeType("application/pdf")
                .xmlEmbedded(true)
                .status(GenerationStatus.COMPLETED)
                .retryCount(1)
                .createdAt(now)
                .completedAt(now)
                .build();
    }

    @Test
    @DisplayName("save() maps domain to entity and back")
    void testSave_roundTrip() {
        AbbreviatedTaxInvoicePdfDocumentEntity entity = AbbreviatedTaxInvoicePdfDocumentEntity.builder()
                .id(id)
                .abbreviatedTaxInvoiceId("ati-123")
                .abbreviatedTaxInvoiceNumber("423-612")
                .documentPath("2026/04/28/abbreviated-tax-invoice-423-612-abc.pdf")
                .documentUrl("http://localhost:9000/abbreviatedtaxinvoices/2026/04/28/abbreviated-tax-invoice-423-612-abc.pdf")
                .fileSize(98765L)
                .mimeType("application/pdf")
                .xmlEmbedded(true)
                .status(GenerationStatus.COMPLETED)
                .retryCount(1)
                .createdAt(LocalDateTime.now())
                .completedAt(LocalDateTime.now())
                .build();
        when(jpaRepository.save(any())).thenReturn(entity);

        AbbreviatedTaxInvoicePdfDocument result = repository.save(domain);

        verify(jpaRepository).save(any(AbbreviatedTaxInvoicePdfDocumentEntity.class));
        assertThat(result.getAbbreviatedTaxInvoiceId()).isEqualTo("ati-123");
        assertThat(result.getAbbreviatedTaxInvoiceNumber()).isEqualTo("423-612");
        assertThat(result.getFileSize()).isEqualTo(98765L);
        assertThat(result.isXmlEmbedded()).isTrue();
        assertThat(result.getStatus()).isEqualTo(GenerationStatus.COMPLETED);
        assertThat(result.getRetryCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("findById() returns mapped domain when found")
    void testFindById_found() {
        AbbreviatedTaxInvoicePdfDocumentEntity entity = AbbreviatedTaxInvoicePdfDocumentEntity.builder()
                .id(id)
                .abbreviatedTaxInvoiceId("ati-123")
                .abbreviatedTaxInvoiceNumber("423-612")
                .status(GenerationStatus.COMPLETED)
                .fileSize(98765L)
                .xmlEmbedded(true)
                .retryCount(0)
                .mimeType("application/pdf")
                .build();
        when(jpaRepository.findById(id)).thenReturn(Optional.of(entity));

        Optional<AbbreviatedTaxInvoicePdfDocument> result = repository.findById(id);

        assertThat(result).isPresent();
        assertThat(result.get().getAbbreviatedTaxInvoiceId()).isEqualTo("ati-123");
    }

    @Test
    @DisplayName("findById() returns empty when not found")
    void testFindById_notFound() {
        when(jpaRepository.findById(id)).thenReturn(Optional.empty());
        assertThat(repository.findById(id)).isEmpty();
    }

    @Test
    @DisplayName("findByAbbreviatedTaxInvoiceId() returns mapped domain when found")
    void testFindByAbbreviatedTaxInvoiceId_found() {
        AbbreviatedTaxInvoicePdfDocumentEntity entity = AbbreviatedTaxInvoicePdfDocumentEntity.builder()
                .id(id)
                .abbreviatedTaxInvoiceId("ati-123")
                .abbreviatedTaxInvoiceNumber("423-612")
                .status(GenerationStatus.PENDING)
                .xmlEmbedded(false)
                .retryCount(0)
                .mimeType("application/pdf")
                .build();
        when(jpaRepository.findByAbbreviatedTaxInvoiceId("ati-123")).thenReturn(Optional.of(entity));

        Optional<AbbreviatedTaxInvoicePdfDocument> result = repository.findByAbbreviatedTaxInvoiceId("ati-123");

        assertThat(result).isPresent();
        assertThat(result.get().getAbbreviatedTaxInvoiceNumber()).isEqualTo("423-612");
    }

    @Test
    @DisplayName("findByAbbreviatedTaxInvoiceId() returns empty when not found")
    void testFindByAbbreviatedTaxInvoiceId_notFound() {
        when(jpaRepository.findByAbbreviatedTaxInvoiceId("unknown")).thenReturn(Optional.empty());
        assertThat(repository.findByAbbreviatedTaxInvoiceId("unknown")).isEmpty();
    }

    @Test
    @DisplayName("deleteById() delegates to JPA repository")
    void testDeleteById() {
        repository.deleteById(id);
        verify(jpaRepository).deleteById(id);
    }

    @Test
    @DisplayName("flush() delegates to JPA repository")
    void testFlush() {
        repository.flush();
        verify(jpaRepository).flush();
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
mvn test -Dtest="JpaAbbreviatedTaxInvoicePdfDocumentRepositoryImplTest" 2>&1 | tail -10
```

Expected: compilation error — entity and adapter classes don't exist yet.

- [ ] **Step 3: Create `AbbreviatedTaxInvoicePdfDocumentEntity`**

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.persistence;

import com.wpanther.abbreviatedtaxinvoice.pdf.domain.model.GenerationStatus;
import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "abbreviated_tax_invoice_pdf_documents", indexes = {
    @Index(name = "idx_ati_pdf_ati_id", columnList = "abbreviated_tax_invoice_id"),
    @Index(name = "idx_ati_pdf_ati_number", columnList = "abbreviated_tax_invoice_number"),
    @Index(name = "idx_ati_pdf_status", columnList = "status")
})
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AbbreviatedTaxInvoicePdfDocumentEntity {

    @Id
    @Column(name = "id", nullable = false)
    private UUID id;

    @Column(name = "abbreviated_tax_invoice_id", nullable = false, length = 100)
    private String abbreviatedTaxInvoiceId;

    @Column(name = "abbreviated_tax_invoice_number", nullable = false, length = 50)
    private String abbreviatedTaxInvoiceNumber;

    @Column(name = "document_path", length = 500)
    private String documentPath;

    @Column(name = "document_url", length = 1000)
    private String documentUrl;

    @Column(name = "file_size")
    private Long fileSize;

    @Column(name = "mime_type", nullable = false, length = 100)
    private String mimeType;

    @Column(name = "xml_embedded", nullable = false)
    private Boolean xmlEmbedded;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    private GenerationStatus status;

    @Column(name = "error_message", columnDefinition = "TEXT")
    private String errorMessage;

    @Column(name = "retry_count")
    private Integer retryCount;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "completed_at")
    private LocalDateTime completedAt;

    @PrePersist
    protected void onCreate() {
        if (id == null)          id = UUID.randomUUID();
        if (status == null)      status = GenerationStatus.PENDING;
        if (mimeType == null)    mimeType = "application/pdf";
        if (xmlEmbedded == null) xmlEmbedded = false;
        if (retryCount == null)  retryCount = 0;
    }
}
```

- [ ] **Step 4: Create `JpaAbbreviatedTaxInvoicePdfDocumentRepository`**

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.persistence;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.Set;
import java.util.UUID;

@Repository
public interface JpaAbbreviatedTaxInvoicePdfDocumentRepository
        extends JpaRepository<AbbreviatedTaxInvoicePdfDocumentEntity, UUID> {

    Optional<AbbreviatedTaxInvoicePdfDocumentEntity> findByAbbreviatedTaxInvoiceId(String abbreviatedTaxInvoiceId);

    @Query("SELECT e.documentPath FROM AbbreviatedTaxInvoicePdfDocumentEntity e WHERE e.documentPath IS NOT NULL")
    Set<String> findAllDocumentPaths();
}
```

- [ ] **Step 5: Create `AbbreviatedTaxInvoicePdfDocumentRepositoryAdapter`**

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.persistence;

import com.wpanther.abbreviatedtaxinvoice.pdf.domain.model.AbbreviatedTaxInvoicePdfDocument;
import com.wpanther.abbreviatedtaxinvoice.pdf.domain.repository.AbbreviatedTaxInvoicePdfDocumentRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
@RequiredArgsConstructor
public class AbbreviatedTaxInvoicePdfDocumentRepositoryAdapter
        implements AbbreviatedTaxInvoicePdfDocumentRepository {

    private final JpaAbbreviatedTaxInvoicePdfDocumentRepository jpaRepository;

    @Override
    public AbbreviatedTaxInvoicePdfDocument save(AbbreviatedTaxInvoicePdfDocument document) {
        return toDomain(jpaRepository.save(toEntity(document)));
    }

    @Override
    public Optional<AbbreviatedTaxInvoicePdfDocument> findById(UUID id) {
        return jpaRepository.findById(id).map(this::toDomain);
    }

    @Override
    public Optional<AbbreviatedTaxInvoicePdfDocument> findByAbbreviatedTaxInvoiceId(String abbreviatedTaxInvoiceId) {
        return jpaRepository.findByAbbreviatedTaxInvoiceId(abbreviatedTaxInvoiceId).map(this::toDomain);
    }

    @Override
    public void deleteById(UUID id) {
        jpaRepository.deleteById(id);
    }

    @Override
    public void flush() {
        jpaRepository.flush();
    }

    private AbbreviatedTaxInvoicePdfDocumentEntity toEntity(AbbreviatedTaxInvoicePdfDocument doc) {
        return AbbreviatedTaxInvoicePdfDocumentEntity.builder()
                .id(doc.getId())
                .abbreviatedTaxInvoiceId(doc.getAbbreviatedTaxInvoiceId())
                .abbreviatedTaxInvoiceNumber(doc.getAbbreviatedTaxInvoiceNumber())
                .documentPath(doc.getDocumentPath())
                .documentUrl(doc.getDocumentUrl())
                .fileSize(doc.getFileSize())
                .mimeType(doc.getMimeType())
                .xmlEmbedded(doc.isXmlEmbedded())
                .status(doc.getStatus())
                .errorMessage(doc.getErrorMessage())
                .retryCount(doc.getRetryCount())
                .createdAt(doc.getCreatedAt())
                .completedAt(doc.getCompletedAt())
                .build();
    }

    private AbbreviatedTaxInvoicePdfDocument toDomain(AbbreviatedTaxInvoicePdfDocumentEntity e) {
        return AbbreviatedTaxInvoicePdfDocument.builder()
                .id(e.getId())
                .abbreviatedTaxInvoiceId(e.getAbbreviatedTaxInvoiceId())
                .abbreviatedTaxInvoiceNumber(e.getAbbreviatedTaxInvoiceNumber())
                .documentPath(e.getDocumentPath())
                .documentUrl(e.getDocumentUrl())
                .fileSize(e.getFileSize() != null ? e.getFileSize() : 0L)
                .mimeType(e.getMimeType())
                .xmlEmbedded(e.getXmlEmbedded() != null && e.getXmlEmbedded())
                .status(e.getStatus())
                .errorMessage(e.getErrorMessage())
                .retryCount(e.getRetryCount() != null ? e.getRetryCount() : 0)
                .createdAt(e.getCreatedAt())
                .completedAt(e.getCompletedAt())
                .build();
    }
}
```

- [ ] **Step 6: Run the tests**

```bash
mvn test -Dtest="JpaAbbreviatedTaxInvoicePdfDocumentRepositoryImplTest" 2>&1 | tail -10
```

Expected: `BUILD SUCCESS`, 7 tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/
git commit -m "feat: add persistence entity, JPA repository, and repository adapter"
```

---

## Task 7: Infrastructure Persistence — Outbox + Flyway Migration

**Files:**
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/persistence/outbox/OutboxEventEntity.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/persistence/outbox/SpringDataOutboxRepository.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/persistence/outbox/JpaOutboxEventRepository.java`
- Create: `src/main/resources/db/migration/V1__create_abbreviated_tax_invoice_pdf_tables.sql`

These classes are identical to the taxinvoice service (the outbox schema is shared). No unit tests required — they are covered by integration tests in `saga-integration-tests`.

- [ ] **Step 1: Create `OutboxEventEntity`** (copy verbatim from taxinvoice, change only the package)

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.persistence.outbox;

import com.wpanther.saga.domain.outbox.OutboxStatus;
import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "outbox_events", indexes = {
    @Index(name = "idx_outbox_status", columnList = "status"),
    @Index(name = "idx_outbox_created", columnList = "created_at"),
    @Index(name = "idx_outbox_aggregate", columnList = "aggregate_id, aggregate_type")
})
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OutboxEventEntity {

    @Id
    @Column(name = "id", nullable = false)
    private UUID id;

    @Column(name = "aggregate_type", nullable = false, length = 100)
    private String aggregateType;

    @Column(name = "aggregate_id", nullable = false, length = 100)
    private String aggregateId;

    @Column(name = "event_type", nullable = false, length = 100)
    private String eventType;

    @Column(name = "payload", nullable = false, columnDefinition = "TEXT")
    private String payload;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    private OutboxStatus status;

    @Column(name = "retry_count")
    private Integer retryCount;

    @Column(name = "error_message", length = 1000)
    private String errorMessage;

    @Column(name = "topic", length = 255)
    private String topic;

    @Column(name = "partition_key", length = 255)
    private String partitionKey;

    @Column(name = "headers", columnDefinition = "TEXT")
    private String headers;

    @PrePersist
    protected void onCreate() {
        if (id == null)         id = UUID.randomUUID();
        if (status == null)     status = OutboxStatus.PENDING;
        if (createdAt == null)  createdAt = Instant.now();
        if (retryCount == null) retryCount = 0;
    }

    public static OutboxEventEntity fromDomain(com.wpanther.saga.domain.outbox.OutboxEvent event) {
        return OutboxEventEntity.builder()
                .id(event.getId())
                .aggregateType(event.getAggregateType())
                .aggregateId(event.getAggregateId())
                .eventType(event.getEventType())
                .payload(event.getPayload())
                .createdAt(event.getCreatedAt())
                .publishedAt(event.getPublishedAt())
                .status(event.getStatus())
                .retryCount(event.getRetryCount())
                .errorMessage(event.getErrorMessage())
                .topic(event.getTopic())
                .partitionKey(event.getPartitionKey())
                .headers(event.getHeaders())
                .build();
    }

    public com.wpanther.saga.domain.outbox.OutboxEvent toDomain() {
        return com.wpanther.saga.domain.outbox.OutboxEvent.builder()
                .id(this.id)
                .aggregateType(this.aggregateType)
                .aggregateId(this.aggregateId)
                .eventType(this.eventType)
                .payload(this.payload)
                .createdAt(this.createdAt)
                .publishedAt(this.publishedAt)
                .status(this.status)
                .retryCount(this.retryCount)
                .errorMessage(this.errorMessage)
                .topic(this.topic)
                .partitionKey(this.partitionKey)
                .headers(this.headers)
                .build();
    }
}
```

- [ ] **Step 2: Create `SpringDataOutboxRepository`**

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.persistence.outbox;

import com.wpanther.saga.domain.outbox.OutboxStatus;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Repository
public interface SpringDataOutboxRepository extends JpaRepository<OutboxEventEntity, UUID> {

    List<OutboxEventEntity> findByStatusOrderByCreatedAtAsc(OutboxStatus status, Pageable pageable);

    @Query("SELECT e FROM OutboxEventEntity e WHERE e.status = 'FAILED' ORDER BY e.createdAt ASC")
    List<OutboxEventEntity> findFailedEventsOrderByCreatedAtAsc(Pageable pageable);

    List<OutboxEventEntity> findByAggregateTypeAndAggregateIdOrderByCreatedAtAsc(
            String aggregateType, String aggregateId);

    @Modifying
    @Query("DELETE FROM OutboxEventEntity e WHERE e.status = 'PUBLISHED' AND e.publishedAt < :before")
    int deletePublishedBefore(@Param("before") Instant before);
}
```

- [ ] **Step 3: Create `JpaOutboxEventRepository`**

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.persistence.outbox;

import com.wpanther.saga.domain.outbox.OutboxEvent;
import com.wpanther.saga.domain.outbox.OutboxEventRepository;
import com.wpanther.saga.domain.outbox.OutboxStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Component
public class JpaOutboxEventRepository implements OutboxEventRepository {

    private static final Logger log = LoggerFactory.getLogger(JpaOutboxEventRepository.class);

    private final SpringDataOutboxRepository springRepository;

    public JpaOutboxEventRepository(SpringDataOutboxRepository springRepository) {
        this.springRepository = springRepository;
    }

    @Override
    public OutboxEvent save(OutboxEvent event) {
        log.debug("Saving outbox event: {} for aggregate: {}/{}",
                event.getId(), event.getAggregateType(), event.getAggregateId());
        return springRepository.save(OutboxEventEntity.fromDomain(event)).toDomain();
    }

    @Override
    public Optional<OutboxEvent> findById(UUID id) {
        return springRepository.findById(id).map(OutboxEventEntity::toDomain);
    }

    @Override
    public List<OutboxEvent> findPendingEvents(int limit) {
        return springRepository.findByStatusOrderByCreatedAtAsc(OutboxStatus.PENDING, Pageable.ofSize(limit))
                .stream().map(OutboxEventEntity::toDomain).toList();
    }

    @Override
    public List<OutboxEvent> findFailedEvents(int limit) {
        return springRepository.findFailedEventsOrderByCreatedAtAsc(Pageable.ofSize(limit))
                .stream().map(OutboxEventEntity::toDomain).toList();
    }

    @Override
    public int deletePublishedBefore(Instant before) {
        int count = springRepository.deletePublishedBefore(before);
        log.info("Deleted {} published outbox events before: {}", count, before);
        return count;
    }

    @Override
    public List<OutboxEvent> findByAggregate(String aggregateType, String aggregateId) {
        return springRepository.findByAggregateTypeAndAggregateIdOrderByCreatedAtAsc(aggregateType, aggregateId)
                .stream().map(OutboxEventEntity::toDomain).toList();
    }
}
```

- [ ] **Step 4: Create Flyway migration `V1__create_abbreviated_tax_invoice_pdf_tables.sql`**

```sql
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
```

- [ ] **Step 5: Verify compilation**

```bash
mvn compile -DskipTests 2>&1 | tail -5
```

Expected: `BUILD SUCCESS`.

- [ ] **Step 6: Commit**

```bash
git add src/
git commit -m "feat: add outbox persistence and Flyway migration"
```

---

<!-- REMAINING TASKS: ask for more tasks in the next turn -->
