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

<!-- REMAINING TASKS: ask for more tasks in the next turn -->
