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

## Task 8: Infrastructure Messaging — Events + Publishers

**Files:**
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/messaging/OutboxConstants.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/messaging/AbbreviatedTaxInvoicePdfGeneratedEvent.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/messaging/AbbreviatedTaxInvoicePdfReplyEvent.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/messaging/EventPublisher.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/messaging/SagaReplyPublisher.java`
- Test: `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/messaging/EventPublisherTest.java`
- Test: `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/messaging/SagaReplyPublisherTest.java`

- [ ] **Step 1: Write the failing tests**

Create `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/messaging/EventPublisherTest.java`:

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.wpanther.saga.infrastructure.outbox.OutboxService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
@DisplayName("EventPublisher Unit Tests")
class EventPublisherTest {

    @Mock
    private OutboxService outboxService;

    private EventPublisher eventPublisher;

    @BeforeEach
    void setUp() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        eventPublisher = new EventPublisher(outboxService, objectMapper);
    }

    @Test
    @DisplayName("publishPdfGenerated() calls OutboxService with correct topic and aggregate type")
    void testPublishPdfGenerated() {
        AbbreviatedTaxInvoicePdfGeneratedEvent event = new AbbreviatedTaxInvoicePdfGeneratedEvent(
                "saga-001", "doc-123", "423-612",
                "http://localhost:9000/abbreviatedtaxinvoices/test.pdf", 98765L, true, "corr-456");

        eventPublisher.publishPdfGenerated(event);

        verify(outboxService).saveWithRouting(
                eq(event),
                eq("AbbreviatedTaxInvoicePdfDocument"),
                eq("doc-123"),
                eq("pdf.generated.abbreviated-tax-invoice"),
                eq("doc-123"),
                anyString()
        );
    }

    @Test
    @DisplayName("publishPdfGenerated() includes documentType header")
    void testPublishPdfGenerated_Headers() {
        AbbreviatedTaxInvoicePdfGeneratedEvent event = new AbbreviatedTaxInvoicePdfGeneratedEvent(
                "saga-001", "doc-123", "423-612",
                "http://localhost:9000/abbreviatedtaxinvoices/test.pdf", 98765L, true, "corr-456");

        eventPublisher.publishPdfGenerated(event);

        ArgumentCaptor<String> headersCaptor = ArgumentCaptor.forClass(String.class);
        verify(outboxService).saveWithRouting(
                any(), anyString(), anyString(), anyString(), anyString(),
                headersCaptor.capture()
        );
        assertThat(headersCaptor.getValue()).contains("\"documentType\":\"ABBREVIATED_TAX_INVOICE\"");
        assertThat(headersCaptor.getValue()).contains("\"correlationId\":\"corr-456\"");
    }

    @Test
    @DisplayName("AbbreviatedTaxInvoicePdfGeneratedEvent stores sagaId and correlationId independently")
    void testSagaIdAndCorrelationIdStoredIndependently() throws Exception {
        ObjectMapper om = new ObjectMapper();
        om.registerModule(new JavaTimeModule());

        AbbreviatedTaxInvoicePdfGeneratedEvent event = new AbbreviatedTaxInvoicePdfGeneratedEvent(
                "saga-001", "doc-123", "423-612",
                "http://localhost:9000/abbreviatedtaxinvoices/test.pdf", 98765L, true, "corr-456");

        assertThat(event.getSagaId()).isEqualTo("saga-001");
        assertThat(event.getCorrelationId()).isEqualTo("corr-456");

        String json = om.writeValueAsString(event);
        AbbreviatedTaxInvoicePdfGeneratedEvent deserialized =
                om.readValue(json, AbbreviatedTaxInvoicePdfGeneratedEvent.class);
        assertThat(deserialized.getSagaId()).isEqualTo("saga-001");
        assertThat(deserialized.getCorrelationId()).isEqualTo("corr-456");
    }
}
```

Create `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/messaging/SagaReplyPublisherTest.java`:

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.wpanther.saga.domain.enums.SagaStep;
import com.wpanther.saga.infrastructure.outbox.OutboxService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
@DisplayName("SagaReplyPublisher Unit Tests")
class SagaReplyPublisherTest {

    @Mock
    private OutboxService outboxService;

    private SagaReplyPublisher sagaReplyPublisher;

    @BeforeEach
    void setUp() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        sagaReplyPublisher = new SagaReplyPublisher(outboxService, objectMapper);
    }

    @Test
    @DisplayName("publishSuccess() routes to correct reply topic with SUCCESS status")
    void testPublishSuccess() {
        sagaReplyPublisher.publishSuccess("saga-1",
                SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF, "corr-1",
                "http://localhost:9000/abbreviatedtaxinvoices/test.pdf", 98765L);

        verify(outboxService).saveWithRouting(
                any(AbbreviatedTaxInvoicePdfReplyEvent.class),
                eq("AbbreviatedTaxInvoicePdfDocument"),
                eq("saga-1"),
                eq("saga.reply.abbreviated-tax-invoice-pdf"),
                eq("saga-1"),
                anyString()
        );
    }

    @Test
    @DisplayName("publishSuccess() reply includes pdfUrl and pdfSize")
    void testPublishSuccess_ReplyPayload() {
        String pdfUrl = "http://localhost:9000/abbreviatedtaxinvoices/test.pdf";
        long pdfSize = 98765L;

        sagaReplyPublisher.publishSuccess("saga-1",
                SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF, "corr-1", pdfUrl, pdfSize);

        ArgumentCaptor<AbbreviatedTaxInvoicePdfReplyEvent> captor =
                ArgumentCaptor.forClass(AbbreviatedTaxInvoicePdfReplyEvent.class);
        verify(outboxService).saveWithRouting(captor.capture(),
                anyString(), anyString(), anyString(), anyString(), anyString());

        assertThat(captor.getValue().isSuccess()).isTrue();
        assertThat(captor.getValue().getPdfUrl()).isEqualTo(pdfUrl);
        assertThat(captor.getValue().getPdfSize()).isEqualTo(pdfSize);
    }

    @Test
    @DisplayName("publishFailure() reply is FAILURE with error message")
    void testPublishFailure() {
        sagaReplyPublisher.publishFailure("saga-1",
                SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF, "corr-1", "FOP render failed");

        ArgumentCaptor<AbbreviatedTaxInvoicePdfReplyEvent> captor =
                ArgumentCaptor.forClass(AbbreviatedTaxInvoicePdfReplyEvent.class);
        verify(outboxService).saveWithRouting(captor.capture(),
                anyString(), anyString(), anyString(), anyString(), anyString());

        assertThat(captor.getValue().isFailure()).isTrue();
        assertThat(captor.getValue().getErrorMessage()).isEqualTo("FOP render failed");
    }

    @Test
    @DisplayName("publishCompensated() reply is COMPENSATED")
    void testPublishCompensated() {
        sagaReplyPublisher.publishCompensated("saga-1",
                SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF, "corr-1");

        ArgumentCaptor<AbbreviatedTaxInvoicePdfReplyEvent> captor =
                ArgumentCaptor.forClass(AbbreviatedTaxInvoicePdfReplyEvent.class);
        verify(outboxService).saveWithRouting(captor.capture(),
                anyString(), anyString(), anyString(), anyString(), anyString());

        assertThat(captor.getValue().isCompensated()).isTrue();
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
mvn test -Dtest="EventPublisherTest,SagaReplyPublisherTest" 2>&1 | tail -10
```

Expected: compilation error — event classes don't exist yet.

- [ ] **Step 3: Create `OutboxConstants`**

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.messaging;

final class OutboxConstants {
    static final String AGGREGATE_TYPE = "AbbreviatedTaxInvoicePdfDocument";
    private OutboxConstants() {}
}
```

- [ ] **Step 4: Create `AbbreviatedTaxInvoicePdfGeneratedEvent`**

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.messaging;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.wpanther.saga.domain.model.TraceEvent;
import lombok.Getter;

import java.time.Instant;
import java.util.UUID;

@Getter
public class AbbreviatedTaxInvoicePdfGeneratedEvent extends TraceEvent {

    private static final String EVENT_TYPE = "pdf.generated.abbreviated-tax-invoice";
    private static final String SOURCE = "abbreviatedtaxinvoice-pdf-generation-service";
    private static final String TRACE_TYPE = "PDF_GENERATED";

    @JsonProperty("documentId")     private final String documentId;
    @JsonProperty("documentNumber") private final String documentNumber;
    @JsonProperty("documentUrl")    private final String documentUrl;
    @JsonProperty("fileSize")       private final long fileSize;
    @JsonProperty("xmlEmbedded")    private final boolean xmlEmbedded;

    public AbbreviatedTaxInvoicePdfGeneratedEvent(
            String sagaId, String documentId, String documentNumber,
            String documentUrl, long fileSize, boolean xmlEmbedded, String correlationId) {
        super(sagaId, correlationId, SOURCE, TRACE_TYPE, null);
        this.documentId     = documentId;
        this.documentNumber = documentNumber;
        this.documentUrl    = documentUrl;
        this.fileSize       = fileSize;
        this.xmlEmbedded    = xmlEmbedded;
    }

    @Override
    public String getEventType() { return EVENT_TYPE; }

    @JsonCreator
    public AbbreviatedTaxInvoicePdfGeneratedEvent(
            @JsonProperty("eventId")       UUID eventId,
            @JsonProperty("occurredAt")    Instant occurredAt,
            @JsonProperty("eventType")     String eventType,
            @JsonProperty("version")       int version,
            @JsonProperty("sagaId")        String sagaId,
            @JsonProperty("correlationId") String correlationId,
            @JsonProperty("source")        String source,
            @JsonProperty("traceType")     String traceType,
            @JsonProperty("context")       String context,
            @JsonProperty("documentId")    String documentId,
            @JsonProperty("documentNumber") String documentNumber,
            @JsonProperty("documentUrl")   String documentUrl,
            @JsonProperty("fileSize")      long fileSize,
            @JsonProperty("xmlEmbedded")   boolean xmlEmbedded) {
        super(eventId, occurredAt, eventType, version, sagaId, correlationId, source, traceType, context);
        this.documentId     = documentId;
        this.documentNumber = documentNumber;
        this.documentUrl    = documentUrl;
        this.fileSize       = fileSize;
        this.xmlEmbedded    = xmlEmbedded;
    }
}
```

- [ ] **Step 5: Create `AbbreviatedTaxInvoicePdfReplyEvent`**

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.messaging;

import com.wpanther.saga.domain.enums.ReplyStatus;
import com.wpanther.saga.domain.enums.SagaStep;
import com.wpanther.saga.domain.model.SagaReply;

public class AbbreviatedTaxInvoicePdfReplyEvent extends SagaReply {

    private static final long serialVersionUID = 1L;

    private String pdfUrl;
    private Long pdfSize;

    public static AbbreviatedTaxInvoicePdfReplyEvent success(
            String sagaId, SagaStep sagaStep, String correlationId,
            String pdfUrl, Long pdfSize) {
        AbbreviatedTaxInvoicePdfReplyEvent reply =
                new AbbreviatedTaxInvoicePdfReplyEvent(sagaId, sagaStep, correlationId, ReplyStatus.SUCCESS);
        reply.pdfUrl = pdfUrl;
        reply.pdfSize = pdfSize;
        return reply;
    }

    public static AbbreviatedTaxInvoicePdfReplyEvent failure(
            String sagaId, SagaStep sagaStep, String correlationId, String errorMessage) {
        return new AbbreviatedTaxInvoicePdfReplyEvent(sagaId, sagaStep, correlationId, errorMessage);
    }

    public static AbbreviatedTaxInvoicePdfReplyEvent compensated(
            String sagaId, SagaStep sagaStep, String correlationId) {
        return new AbbreviatedTaxInvoicePdfReplyEvent(sagaId, sagaStep, correlationId, ReplyStatus.COMPENSATED);
    }

    private AbbreviatedTaxInvoicePdfReplyEvent(
            String sagaId, SagaStep sagaStep, String correlationId, ReplyStatus status) {
        super(sagaId, sagaStep, correlationId, status);
    }

    private AbbreviatedTaxInvoicePdfReplyEvent(
            String sagaId, SagaStep sagaStep, String correlationId, String errorMessage) {
        super(sagaId, sagaStep, correlationId, errorMessage);
    }

    public String getPdfUrl()  { return pdfUrl; }
    public Long getPdfSize()   { return pdfSize; }
}
```

- [ ] **Step 6: Create `EventPublisher`**

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wpanther.abbreviatedtaxinvoice.pdf.application.port.out.PdfEventPort;
import com.wpanther.saga.infrastructure.outbox.OutboxService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;

@Component
@RequiredArgsConstructor
@Slf4j
public class EventPublisher implements PdfEventPort {

    private final OutboxService outboxService;
    private final ObjectMapper objectMapper;

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void publishPdfGenerated(AbbreviatedTaxInvoicePdfGeneratedEvent event) {
        Map<String, String> headers = Map.of(
                "documentType", "ABBREVIATED_TAX_INVOICE",
                "correlationId", event.getCorrelationId()
        );
        outboxService.saveWithRouting(
                event,
                OutboxConstants.AGGREGATE_TYPE,
                event.getDocumentId(),
                "pdf.generated.abbreviated-tax-invoice",
                event.getDocumentId(),
                toJson(headers)
        );
        log.info("Published AbbreviatedTaxInvoicePdfGeneratedEvent for: {}", event.getDocumentNumber());
    }

    private String toJson(Map<String, String> map) {
        try {
            return objectMapper.writeValueAsString(map);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize outbox event headers", e);
        }
    }
}
```

- [ ] **Step 7: Create `SagaReplyPublisher`**

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wpanther.abbreviatedtaxinvoice.pdf.application.port.out.SagaReplyPort;
import com.wpanther.saga.domain.enums.SagaStep;
import com.wpanther.saga.infrastructure.outbox.OutboxService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;

@Component
@RequiredArgsConstructor
@Slf4j
public class SagaReplyPublisher implements SagaReplyPort {

    private static final String REPLY_TOPIC = "saga.reply.abbreviated-tax-invoice-pdf";

    private final OutboxService outboxService;
    private final ObjectMapper objectMapper;

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void publishSuccess(String sagaId, SagaStep sagaStep, String correlationId,
                               String pdfUrl, long pdfSize) {
        AbbreviatedTaxInvoicePdfReplyEvent reply =
                AbbreviatedTaxInvoicePdfReplyEvent.success(sagaId, sagaStep, correlationId, pdfUrl, pdfSize);
        outboxService.saveWithRouting(reply, OutboxConstants.AGGREGATE_TYPE, sagaId,
                REPLY_TOPIC, sagaId, toJson(Map.of("sagaId", sagaId, "correlationId", correlationId, "status", "SUCCESS")));
        log.info("Published SUCCESS saga reply for saga {} step {}", sagaId, sagaStep);
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void publishFailure(String sagaId, SagaStep sagaStep, String correlationId, String errorMessage) {
        AbbreviatedTaxInvoicePdfReplyEvent reply =
                AbbreviatedTaxInvoicePdfReplyEvent.failure(sagaId, sagaStep, correlationId, errorMessage);
        outboxService.saveWithRouting(reply, OutboxConstants.AGGREGATE_TYPE, sagaId,
                REPLY_TOPIC, sagaId, toJson(Map.of("sagaId", sagaId, "correlationId", correlationId, "status", "FAILURE")));
        log.info("Published FAILURE saga reply for saga {} step {}: {}", sagaId, sagaStep, errorMessage);
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void publishCompensated(String sagaId, SagaStep sagaStep, String correlationId) {
        AbbreviatedTaxInvoicePdfReplyEvent reply =
                AbbreviatedTaxInvoicePdfReplyEvent.compensated(sagaId, sagaStep, correlationId);
        outboxService.saveWithRouting(reply, OutboxConstants.AGGREGATE_TYPE, sagaId,
                REPLY_TOPIC, sagaId, toJson(Map.of("sagaId", sagaId, "correlationId", correlationId, "status", "COMPENSATED")));
        log.info("Published COMPENSATED saga reply for saga {} step {}", sagaId, sagaStep);
    }

    private String toJson(Map<String, String> map) {
        try {
            return objectMapper.writeValueAsString(map);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize outbox event headers", e);
        }
    }
}
```

- [ ] **Step 8: Run the tests**

```bash
mvn test -Dtest="EventPublisherTest,SagaReplyPublisherTest" 2>&1 | tail -10
```

Expected: `BUILD SUCCESS`, 7 tests pass.

- [ ] **Step 9: Commit**

```bash
git add src/
git commit -m "feat: add messaging events, reply event, and outbox publishers"
```

---

## Task 9: Kafka Commands, Mapper, and Camel Route Config

**Files:**
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/in/kafka/KafkaAbbreviatedTaxInvoiceProcessCommand.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/in/kafka/KafkaAbbreviatedTaxInvoiceCompensateCommand.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/in/kafka/KafkaCommandMapper.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/in/kafka/SagaRouteConfig.java`
- Test: `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/in/kafka/KafkaCommandMapperTest.java`

- [ ] **Step 1: Write the failing test**

Create `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/in/kafka/KafkaCommandMapperTest.java`:

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.in.kafka;

import com.wpanther.saga.domain.enums.SagaStep;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class KafkaCommandMapperTest {

    private final KafkaCommandMapper mapper = new KafkaCommandMapper();

    @Test
    void toProcess_mapsAllFields() {
        var src = new KafkaAbbreviatedTaxInvoiceProcessCommand(
                null, null, null, 0,
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
                null, null, null, 0,
                "saga-2", SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF, "corr-2",
                "doc-2");

        var result = mapper.toCompensate(src);

        assertThat(result.getSagaId()).isEqualTo("saga-2");
        assertThat(result.getCorrelationId()).isEqualTo("corr-2");
        assertThat(result.getDocumentId()).isEqualTo("doc-2");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
mvn test -Dtest="KafkaCommandMapperTest" 2>&1 | tail -10
```

Expected: compilation error — command classes don't exist yet.

- [ ] **Step 3: Create `KafkaAbbreviatedTaxInvoiceProcessCommand`**

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.in.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.wpanther.saga.domain.enums.SagaStep;
import com.wpanther.saga.domain.model.SagaCommand;
import lombok.Getter;

import java.time.Instant;
import java.util.UUID;

public class KafkaAbbreviatedTaxInvoiceProcessCommand extends SagaCommand {

    private static final long serialVersionUID = 1L;

    @Getter @JsonProperty("documentId")     private final String documentId;
    @Getter @JsonProperty("documentNumber") private final String documentNumber;
    @Getter @JsonProperty("signedXmlUrl")   private final String signedXmlUrl;

    @JsonCreator
    public KafkaAbbreviatedTaxInvoiceProcessCommand(
            @JsonProperty("eventId")        UUID eventId,
            @JsonProperty("occurredAt")     Instant occurredAt,
            @JsonProperty("eventType")      String eventType,
            @JsonProperty("version")        int version,
            @JsonProperty("sagaId")         String sagaId,
            @JsonProperty("sagaStep")       SagaStep sagaStep,
            @JsonProperty("correlationId")  String correlationId,
            @JsonProperty("documentId")     String documentId,
            @JsonProperty("documentNumber") String documentNumber,
            @JsonProperty("signedXmlUrl")   String signedXmlUrl) {
        super(eventId, occurredAt, eventType, version, sagaId, sagaStep, correlationId);
        this.documentId     = documentId;
        this.documentNumber = documentNumber;
        this.signedXmlUrl   = signedXmlUrl;
    }

    /** Convenience constructor for testing. */
    public KafkaAbbreviatedTaxInvoiceProcessCommand(
            String sagaId, SagaStep sagaStep, String correlationId,
            String documentId, String documentNumber, String signedXmlUrl) {
        super(sagaId, sagaStep, correlationId);
        this.documentId     = documentId;
        this.documentNumber = documentNumber;
        this.signedXmlUrl   = signedXmlUrl;
    }

    @Override public String getSagaId()        { return super.getSagaId(); }
    @Override public SagaStep getSagaStep()    { return super.getSagaStep(); }
    @Override public String getCorrelationId() { return super.getCorrelationId(); }
}
```

- [ ] **Step 4: Create `KafkaAbbreviatedTaxInvoiceCompensateCommand`**

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.in.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.wpanther.saga.domain.enums.SagaStep;
import com.wpanther.saga.domain.model.SagaCommand;
import lombok.Getter;

import java.time.Instant;
import java.util.UUID;

public class KafkaAbbreviatedTaxInvoiceCompensateCommand extends SagaCommand {

    private static final long serialVersionUID = 1L;

    @Getter @JsonProperty("documentId") private final String documentId;

    @JsonCreator
    public KafkaAbbreviatedTaxInvoiceCompensateCommand(
            @JsonProperty("eventId")       UUID eventId,
            @JsonProperty("occurredAt")    Instant occurredAt,
            @JsonProperty("eventType")     String eventType,
            @JsonProperty("version")       int version,
            @JsonProperty("sagaId")        String sagaId,
            @JsonProperty("sagaStep")      SagaStep sagaStep,
            @JsonProperty("correlationId") String correlationId,
            @JsonProperty("documentId")    String documentId) {
        super(eventId, occurredAt, eventType, version, sagaId, sagaStep, correlationId);
        this.documentId = documentId;
    }

    /** Convenience constructor for testing. */
    public KafkaAbbreviatedTaxInvoiceCompensateCommand(
            String sagaId, SagaStep sagaStep, String correlationId, String documentId) {
        super(sagaId, sagaStep, correlationId);
        this.documentId = documentId;
    }

    @Override public String getSagaId()        { return super.getSagaId(); }
    @Override public SagaStep getSagaStep()    { return super.getSagaStep(); }
    @Override public String getCorrelationId() { return super.getCorrelationId(); }
    public String getDocumentId()              { return documentId; }
}
```

- [ ] **Step 5: Create `KafkaCommandMapper`**

```java
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
```

- [ ] **Step 6: Run the mapper test**

```bash
mvn test -Dtest="KafkaCommandMapperTest" 2>&1 | tail -10
```

Expected: `BUILD SUCCESS`, 2 tests pass.

- [ ] **Step 7: Create `SagaRouteConfig`** (Camel routes — no unit test; tested by `CamelRouteConfigTest` in Task 12)

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.in.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wpanther.abbreviatedtaxinvoice.pdf.application.service.SagaCommandHandler;
import com.wpanther.abbreviatedtaxinvoice.pdf.application.usecase.CompensateAbbreviatedTaxInvoicePdfUseCase;
import com.wpanther.abbreviatedtaxinvoice.pdf.application.usecase.ProcessAbbreviatedTaxInvoicePdfUseCase;
import com.wpanther.saga.domain.enums.SagaStep;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
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
        this.processUseCase     = processUseCase;
        this.compensateUseCase  = compensateUseCase;
        this.sagaCommandHandler = sagaCommandHandler;
        this.objectMapper       = objectMapper;
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
                            if (body instanceof KafkaAbbreviatedTaxInvoiceProcessCommand cmd) {
                                log.error("DLQ: notifying orchestrator of retry exhaustion for saga {} document {}",
                                        cmd.getSagaId(), cmd.getDocumentNumber());
                                sagaCommandHandler.publishOrchestrationFailure(cmd, cause);
                            } else if (body instanceof KafkaAbbreviatedTaxInvoiceCompensateCommand cmd) {
                                log.error("DLQ: notifying orchestrator of compensation retry exhaustion for saga {}",
                                        cmd.getSagaId());
                                sagaCommandHandler.publishCompensationOrchestrationFailure(cmd, cause);
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
                .unmarshal().json(JsonLibrary.Jackson, KafkaAbbreviatedTaxInvoiceProcessCommand.class)
                .process(exchange -> {
                    KafkaAbbreviatedTaxInvoiceProcessCommand cmd =
                            exchange.getIn().getBody(KafkaAbbreviatedTaxInvoiceProcessCommand.class);
                    log.info("Processing saga command for saga: {}, document: {}",
                            cmd.getSagaId(), cmd.getDocumentNumber());
                    processUseCase.handle(cmd);
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
                .unmarshal().json(JsonLibrary.Jackson, KafkaAbbreviatedTaxInvoiceCompensateCommand.class)
                .process(exchange -> {
                    KafkaAbbreviatedTaxInvoiceCompensateCommand cmd =
                            exchange.getIn().getBody(KafkaAbbreviatedTaxInvoiceCompensateCommand.class);
                    log.info("Processing compensation for saga: {}, document: {}",
                            cmd.getSagaId(), cmd.getDocumentId());
                    compensateUseCase.handle(cmd);
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
            JsonNode node        = objectMapper.readTree(rawBytes);
            String sagaId        = node.path("sagaId").asText(null);
            String sagaStepStr   = node.path("sagaStep").asText(null);
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
```

- [ ] **Step 8: Verify compilation**

```bash
mvn compile -DskipTests 2>&1 | tail -5
```

Expected: `BUILD SUCCESS`.

- [ ] **Step 9: Commit**

```bash
git add src/
git commit -m "feat: add Kafka command DTOs, mapper, and Camel route config"
```

---

## Task 10: Configuration Classes + Metrics

**Files:**
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/config/MinioConfig.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/config/OutboxConfig.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/config/RestTemplateConfig.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/config/FontHealthCheck.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/metrics/PdfGenerationMetrics.java`
- Test: `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/config/FontHealthCheckTest.java`

These config classes are identical to the taxinvoice service (only the package and log messages differ). No tests for MinioConfig/OutboxConfig/RestTemplateConfig — they are thin wiring classes covered by context load tests.

- [ ] **Step 1: Create `MinioConfig`**

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;

@Configuration
@Slf4j
public class MinioConfig {

    @Bean
    public S3Client s3Client(
            @Value("${app.minio.endpoint}")               String endpoint,
            @Value("${app.minio.access-key}")             String accessKey,
            @Value("${app.minio.secret-key}")             String secretKey,
            @Value("${app.minio.region}")                 String region,
            @Value("${app.minio.path-style-access:true}") boolean pathStyleAccess) {

        AwsBasicCredentials creds = AwsBasicCredentials.create(accessKey, secretKey);
        S3Client client = S3Client.builder()
                .region(Region.of(region))
                .credentialsProvider(StaticCredentialsProvider.create(creds))
                .endpointOverride(URI.create(endpoint))
                .forcePathStyle(pathStyleAccess)
                .build();
        log.info("Initialized MinIO S3 client: endpoint={}", endpoint);
        return client;
    }
}
```

- [ ] **Step 2: Create `OutboxConfig`**

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.persistence.outbox.JpaOutboxEventRepository;
import com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.persistence.outbox.SpringDataOutboxRepository;
import com.wpanther.saga.domain.outbox.OutboxEventRepository;
import com.wpanther.saga.infrastructure.outbox.OutboxService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OutboxConfig {

    @Bean
    @ConditionalOnMissingBean(OutboxEventRepository.class)
    public OutboxEventRepository outboxEventRepository(SpringDataOutboxRepository springRepository) {
        return new JpaOutboxEventRepository(springRepository);
    }

    @Bean
    @ConditionalOnMissingBean(OutboxService.class)
    public OutboxService outboxService(OutboxEventRepository repository, ObjectMapper objectMapper) {
        return new OutboxService(repository, objectMapper);
    }
}
```

- [ ] **Step 3: Create `RestTemplateConfig`**

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.config;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

@Configuration
@Slf4j
public class RestTemplateConfig {

    @Value("${app.rest-client.connect-timeout:5000}") private int connectTimeout;
    @Value("${app.rest-client.read-timeout:30000}")   private int readTimeout;

    private final CircuitBreakerRegistry circuitBreakerRegistry;

    public RestTemplateConfig(CircuitBreakerRegistry circuitBreakerRegistry) {
        this.circuitBreakerRegistry = circuitBreakerRegistry;
    }

    @PostConstruct
    public void setupCircuitBreakerLogging() {
        CircuitBreaker signedXmlFetch = circuitBreakerRegistry.circuitBreaker("signedXmlFetch");
        CircuitBreaker minio          = circuitBreakerRegistry.circuitBreaker("minio");
        signedXmlFetch.getEventPublisher().onStateTransition(e ->
                log.info("Circuit breaker 'signedXmlFetch' state transition: {}", e.getStateTransition()));
        minio.getEventPublisher().onStateTransition(e ->
                log.info("Circuit breaker 'minio' state transition: {}", e.getStateTransition()));
        log.info("Circuit breaker event logging configured");
    }

    @Bean
    @ConditionalOnMissingBean(RestTemplate.class)
    public RestTemplate restTemplate() {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(connectTimeout);
        factory.setReadTimeout(readTimeout);
        log.info("Configured RestTemplate: connectTimeout={}ms, readTimeout={}ms", connectTimeout, readTimeout);
        return new RestTemplate(factory);
    }
}
```

- [ ] **Step 4: Create `FontHealthCheck`**

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
@Slf4j
@ConditionalOnProperty(name = "app.fonts.health-check.enabled", havingValue = "true", matchIfMissing = true)
public class FontHealthCheck {

    private static final String[] REQUIRED_FONTS = {
            "fonts/THSarabunNew.ttf",
            "fonts/THSarabunNew-Bold.ttf",
            "fonts/THSarabunNew-Italic.ttf",
            "fonts/THSarabunNew-BoldItalic.ttf",
            "fonts/NotoSansThaiLooped-Regular.ttf",
            "fonts/NotoSansThaiLooped-Bold.ttf"
    };

    @Value("${app.fonts.health-check.fail-on-error:true}")
    private boolean failOnError;

    @EventListener(ApplicationReadyEvent.class)
    public void checkFontsAtStartup() {
        log.info("Performing font health check...");
        List<String> missing = new ArrayList<>();
        for (String fontPath : REQUIRED_FONTS) {
            if (!new ClassPathResource(fontPath).exists()) {
                missing.add(fontPath);
                log.warn("Required font missing: {}", fontPath);
            }
        }
        if (missing.isEmpty()) {
            log.info("Font health check passed: all {} fonts present", REQUIRED_FONTS.length);
            return;
        }
        String msg = String.format("Missing %d of %d required Thai fonts: %s. "
                + "Place fonts in src/main/resources/fonts/ or disable check with "
                + "app.fonts.health-check.enabled=false.",
                missing.size(), REQUIRED_FONTS.length, missing);
        if (failOnError) {
            log.error("Font health check failed: {}", msg);
            throw new IllegalStateException(msg);
        }
        log.warn("Font health check failed but continuing: {}", msg);
    }
}
```

- [ ] **Step 5: Create `PdfGenerationMetrics`**

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class PdfGenerationMetrics {

    private static final String RETRY_EXHAUSTED_COUNTER = "pdf.generation.retry.exhausted";

    private final MeterRegistry meterRegistry;

    public PdfGenerationMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        Counter.builder(RETRY_EXHAUSTED_COUNTER)
                .description("Number of times PDF generation max retries were exceeded")
                .register(meterRegistry);
    }

    public void recordRetryExhausted(String sagaId, String abbreviatedTaxInvoiceId,
                                     String abbreviatedTaxInvoiceNumber) {
        meterRegistry.counter(RETRY_EXHAUSTED_COUNTER,
                "saga_id", sagaId,
                "abbreviated_tax_invoice_id", abbreviatedTaxInvoiceId,
                "abbreviated_tax_invoice_number", abbreviatedTaxInvoiceNumber)
                .increment();
    }
}
```

- [ ] **Step 6: Create `FontHealthCheckTest`**

Create `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/config/FontHealthCheckTest.java`:

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

@DisplayName("FontHealthCheck Tests")
class FontHealthCheckTest {

    private FontHealthCheck healthCheck(boolean failOnError) {
        FontHealthCheck check = new FontHealthCheck();
        // Inject failOnError via reflection (field injection in prod; testing directly)
        try {
            var field = FontHealthCheck.class.getDeclaredField("failOnError");
            field.setAccessible(true);
            field.set(check, failOnError);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return check;
    }

    @Test
    @DisplayName("checkFontsAtStartup() throws when fonts are missing and failOnError=true")
    void testFontsAbsent_FailOnError() {
        FontHealthCheck check = healthCheck(true);
        // Fonts not on classpath in test environment (no fonts/ directory in test resources)
        assertThatThrownBy(check::checkFontsAtStartup)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Missing");
    }

    @Test
    @DisplayName("checkFontsAtStartup() logs warning only when fonts are missing and failOnError=false")
    void testFontsAbsent_WarnOnly() {
        FontHealthCheck check = healthCheck(false);
        // Should not throw even though fonts are missing
        assertThatCode(check::checkFontsAtStartup).doesNotThrowAnyException();
    }
}
```

- [ ] **Step 7: Run the test**

```bash
mvn test -Dtest="FontHealthCheckTest" 2>&1 | tail -10
```

Expected: `BUILD SUCCESS`, 2 tests pass.

- [ ] **Step 8: Verify full compile**

```bash
mvn compile -DskipTests 2>&1 | tail -5
```

Expected: `BUILD SUCCESS`.

- [ ] **Step 9: Commit**

```bash
git add src/
git commit -m "feat: add infrastructure config classes and metrics"
```

---

---

## Task 11: PDF Infrastructure — FOP Generator + PDF/A-3 Converter + Amount Words + ServiceImpl

**Files:**
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/pdf/FopAbbreviatedTaxInvoicePdfGenerator.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/pdf/PdfA3Converter.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/pdf/ThaiAmountWordsConverter.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/pdf/AbbreviatedTaxInvoicePdfGenerationServiceImpl.java`
- Test: `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/pdf/FopAbbreviatedTaxInvoicePdfGeneratorTest.java`
- Test: `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/pdf/PdfA3ConverterTest.java`

- [ ] **Step 1: Create `FopAbbreviatedTaxInvoicePdfGenerator`**

Create `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/pdf/FopAbbreviatedTaxInvoicePdfGenerator.java`:

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.pdf;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.tracing.annotation.NewSpan;
import lombok.extern.slf4j.Slf4j;
import org.apache.fop.apps.Fop;
import org.apache.fop.apps.FopFactory;
import org.apache.fop.apps.MimeConstants;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Templates;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXResult;
import javax.xml.transform.stream.StreamSource;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class FopAbbreviatedTaxInvoicePdfGenerator {

    private static final String FOP_CONFIG_PATH = "fop/fop.xconf";
    private static final String XSL_PATH = "xsl/abbreviatedtaxinvoice-direct.xsl";

    private static final List<String> REQUIRED_FONTS = List.of(
            "fonts/THSarabunNew.ttf",
            "fonts/THSarabunNew-Bold.ttf"
    );

    private final FopFactory fopFactory;
    private final Templates cachedTemplates;
    private final Semaphore renderSemaphore;
    private final Timer renderTimer;
    private final DistributionSummary pdfSizeSummary;
    private final long maxPdfSizeBytes;

    public FopAbbreviatedTaxInvoicePdfGenerator(
            @Value("${app.pdf.generation.max-concurrent-renders:3}") int maxConcurrentRenders,
            @Value("${app.pdf.generation.max-pdf-size-bytes:52428800}") long maxPdfSizeBytes,
            MeterRegistry meterRegistry) {
        if (maxConcurrentRenders < 1) {
            throw new IllegalStateException(
                    "app.pdf.generation.max-concurrent-renders must be >= 1, got: " + maxConcurrentRenders);
        }
        if (maxPdfSizeBytes < 1) {
            throw new IllegalStateException(
                    "app.pdf.generation.max-pdf-size-bytes must be >= 1, got: " + maxPdfSizeBytes);
        }
        this.maxPdfSizeBytes = maxPdfSizeBytes;
        try {
            this.fopFactory = createFopFactory();
            TransformerFactory tf = TransformerFactory.newInstance();
            this.cachedTemplates = compileTemplates(tf, XSL_PATH);
            this.renderSemaphore = new Semaphore(maxConcurrentRenders, true);

            this.renderTimer = meterRegistry.timer("pdf.fop.render");
            this.pdfSizeSummary = DistributionSummary.builder("pdf.fop.size.bytes")
                    .description("Size of generated abbreviated tax invoice PDFs in bytes")
                    .register(meterRegistry);
            Gauge.builder("pdf.fop.render.available_permits", renderSemaphore, Semaphore::availablePermits)
                    .description("Available FOP concurrent render permits")
                    .register(meterRegistry);

            log.info("FopAbbreviatedTaxInvoicePdfGenerator initialized: maxConcurrentRenders={} maxPdfSizeBytes={}",
                    maxConcurrentRenders, maxPdfSizeBytes);
            checkFontAvailability();
        } catch (Exception e) {
            throw new PdfInitializationException("Failed to initialize FOP PDF generator: " + e.getMessage(), e);
        }
    }

    private Templates compileTemplates(TransformerFactory tf, String xslPath) throws Exception {
        ClassPathResource xslResource = new ClassPathResource(xslPath);
        if (!xslResource.exists()) {
            throw new IllegalStateException("XSL template not found at startup: " + xslPath);
        }
        try (InputStream is = xslResource.getInputStream()) {
            return tf.newTemplates(new StreamSource(is));
        }
    }

    private FopFactory createFopFactory() throws Exception {
        URI baseUri = resolveBaseUri();
        try {
            ClassPathResource configResource = new ClassPathResource(FOP_CONFIG_PATH);
            if (configResource.exists()) {
                try (InputStream configStream = configResource.getInputStream()) {
                    return FopFactory.newInstance(baseUri, configStream);
                }
            } else {
                log.warn("FOP config not found at {}, using default configuration", FOP_CONFIG_PATH);
                return FopFactory.newInstance(baseUri);
            }
        } catch (Exception e) {
            log.warn("Failed to load FOP config, using default: {}", e.getMessage());
            return FopFactory.newInstance(baseUri);
        }
    }

    private URI resolveBaseUri() {
        try {
            URL classpathRoot = new ClassPathResource("").getURL();
            URI uri = classpathRoot.toURI();
            log.debug("FOP base URI resolved to: {}", uri);
            return uri;
        } catch (Exception e) {
            log.warn("Could not resolve classpath root URI for FOP, falling back to working directory: {}",
                    e.getMessage());
            return URI.create("file:" + System.getProperty("user.dir", ".") + "/");
        }
    }

    public void checkFontAvailability() {
        List<String> missing = REQUIRED_FONTS.stream()
                .filter(font -> !new ClassPathResource(font).exists())
                .toList();
        if (!missing.isEmpty()) {
            log.warn("Thai font files not found on classpath: {} — Thai text may not render correctly.", missing);
        } else {
            log.info("Font check: all {} required Thai font files present on classpath.", REQUIRED_FONTS.size());
        }
    }

    @NewSpan("pdf.fop.render")
    public byte[] generatePdf(String xmlData, Map<String, Object> params) throws PdfGenerationException {
        log.debug("Awaiting render permit (available={})", renderSemaphore.availablePermits());
        try {
            renderSemaphore.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PdfGenerationException("PDF generation interrupted while waiting for render slot", e);
        }
        long t0 = System.nanoTime();
        try {
            Transformer transformer = cachedTemplates.newTransformer();
            if (params != null) {
                params.forEach(transformer::setParameter);
            }
            return renderPdf(xmlData, transformer);
        } catch (javax.xml.transform.TransformerConfigurationException e) {
            throw new PdfGenerationException("Failed to create transformer: " + e.getMessage(), e);
        } finally {
            try {
                renderTimer.record(System.nanoTime() - t0, TimeUnit.NANOSECONDS);
            } finally {
                renderSemaphore.release();
            }
        }
    }

    public byte[] generatePdf(String xmlData) throws PdfGenerationException {
        return generatePdf(xmlData, null);
    }

    private byte[] renderPdf(String xmlData, Transformer transformer) throws PdfGenerationException {
        try (ByteArrayOutputStream pdfOutput = new ByteArrayOutputStream()) {
            Fop fop = fopFactory.newFop(MimeConstants.MIME_PDF, pdfOutput);
            Source xmlSource = new StreamSource(
                new ByteArrayInputStream(xmlData.getBytes(StandardCharsets.UTF_8))
            );
            Result result = new SAXResult(fop.getDefaultHandler());
            transformer.transform(xmlSource, result);
            byte[] pdfBytes = pdfOutput.toByteArray();

            if (pdfBytes.length > maxPdfSizeBytes) {
                throw new PdfGenerationException(
                        String.format("Generated PDF exceeds max allowed size: %d bytes > %d bytes",
                                pdfBytes.length, maxPdfSizeBytes));
            }

            log.info("Generated PDF: {} bytes", pdfBytes.length);
            pdfSizeSummary.record(pdfBytes.length);
            return pdfBytes;
        } catch (Exception e) {
            log.error("Failed to generate PDF", e);
            throw new PdfGenerationException("PDF generation failed: " + e.getMessage(), e);
        }
    }

    public static class PdfGenerationException extends Exception {
        public PdfGenerationException(String message) { super(message); }
        public PdfGenerationException(String message, Throwable cause) { super(message, cause); }
    }

    public static class PdfInitializationException extends RuntimeException {
        public PdfInitializationException(String message) { super(message); }
        public PdfInitializationException(String message, Throwable cause) { super(message, cause); }
    }
}
```

- [ ] **Step 2: Create `PdfA3Converter`**

Create `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/pdf/PdfA3Converter.java`:

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.pdf;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.apache.pdfbox.Loader;
import org.apache.pdfbox.cos.COSName;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDDocumentCatalog;
import org.apache.pdfbox.pdmodel.PDDocumentNameDictionary;
import org.apache.pdfbox.pdmodel.PDEmbeddedFilesNameTreeNode;
import org.apache.pdfbox.pdmodel.common.PDMetadata;
import org.apache.pdfbox.pdmodel.common.filespecification.PDComplexFileSpecification;
import org.apache.pdfbox.pdmodel.common.filespecification.PDEmbeddedFile;
import org.apache.pdfbox.pdmodel.graphics.color.PDOutputIntent;
import org.apache.xmpbox.XMPMetadata;
import org.apache.xmpbox.schema.DublinCoreSchema;
import org.apache.xmpbox.schema.PDFAIdentificationSchema;
import org.apache.xmpbox.schema.XMPBasicSchema;
import org.apache.xmpbox.xml.XmpSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class PdfA3Converter {

    private static final String MIME_TYPE_XML = "application/xml";
    private static final String AFRelationship_SOURCE = "Source";

    private final String iccProfilePath;
    private final Timer conversionTimer;

    public PdfA3Converter(@Value("${app.pdf.icc-profile-path:icc/sRGB.icc}") String iccProfilePath,
                          MeterRegistry meterRegistry) {
        this.iccProfilePath = iccProfilePath;
        this.conversionTimer = meterRegistry.timer("pdf.conversion.pdfa3");
        loadIccProfile();
    }

    public byte[] convertToPdfA3(byte[] pdfBytes, String xmlContent, String xmlFilename,
                                  String abbreviatedTaxInvoiceNumber) throws PdfConversionException {
        log.debug("Converting PDF to PDF/A-3 with embedded XML: {}", xmlFilename);
        long t0 = System.nanoTime();
        try (PDDocument document = Loader.loadPDF(pdfBytes)) {
            addPdfAMetadata(document, abbreviatedTaxInvoiceNumber);
            addColorProfile(document);
            embedXmlFile(document, xmlContent, xmlFilename);
            try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
                document.save(output);
                byte[] result = output.toByteArray();
                log.info("Converted to PDF/A-3: {} bytes (XML embedded: {})", result.length, xmlFilename);
                return result;
            }
        } catch (Exception e) {
            log.error("Failed to convert PDF to PDF/A-3", e);
            throw new PdfConversionException("PDF/A-3 conversion failed: " + e.getMessage(), e);
        } finally {
            conversionTimer.record(System.nanoTime() - t0, TimeUnit.NANOSECONDS);
        }
    }

    private void addPdfAMetadata(PDDocument document, String abbreviatedTaxInvoiceNumber) throws Exception {
        XMPMetadata xmp = XMPMetadata.createXMPMetadata();

        PDFAIdentificationSchema pdfaId = xmp.createAndAddPDFAIdentificationSchema();
        pdfaId.setPart(3);
        pdfaId.setConformance("B");

        DublinCoreSchema dc = xmp.createAndAddDublinCoreSchema();
        dc.setTitle("Thai e-Tax Abbreviated Tax Invoice: " + abbreviatedTaxInvoiceNumber);
        dc.setDescription("Electronic abbreviated tax invoice with embedded XML source");
        dc.addCreator("Abbreviated Tax Invoice PDF Generation Service");
        dc.setFormat("application/pdf");

        XMPBasicSchema xmpBasic = xmp.createAndAddXMPBasicSchema();
        Calendar now = GregorianCalendar.from(LocalDateTime.now().atZone(ZoneId.systemDefault()));
        xmpBasic.setCreateDate(now);
        xmpBasic.setModifyDate(now);
        xmpBasic.setCreatorTool("Thai e-Tax Abbreviated Tax Invoice System");

        XmpSerializer serializer = new XmpSerializer();
        ByteArrayOutputStream xmpOutput = new ByteArrayOutputStream();
        serializer.serialize(xmp, xmpOutput, true);

        PDMetadata metadata = new PDMetadata(document);
        metadata.importXMPMetadata(xmpOutput.toByteArray());
        document.getDocumentCatalog().setMetadata(metadata);
    }

    private void addColorProfile(PDDocument document) throws Exception {
        PDDocumentCatalog catalog = document.getDocumentCatalog();
        if (catalog.getOutputIntents() != null && !catalog.getOutputIntents().isEmpty()) {
            return;
        }
        try (InputStream iccStream = loadIccProfile()) {
            PDOutputIntent outputIntent = new PDOutputIntent(document, iccStream);
            outputIntent.setInfo("sRGB IEC61966-2.1");
            outputIntent.setOutputCondition("sRGB");
            outputIntent.setOutputConditionIdentifier("sRGB IEC61966-2.1");
            outputIntent.setRegistryName("http://www.color.org");
            catalog.addOutputIntent(outputIntent);
        }
    }

    private InputStream loadIccProfile() {
        ClassPathResource iccResource = new ClassPathResource(iccProfilePath);
        if (iccResource.exists()) {
            try {
                return iccResource.getInputStream();
            } catch (Exception e) {
                throw new IllegalStateException(
                        "Failed to read ICC profile from " + iccProfilePath + ": " + e.getMessage(), e);
            }
        }
        throw new IllegalStateException(
                "ICC profile not found on classpath: " + iccProfilePath
                + " — add sRGB.icc to src/main/resources/icc/");
    }

    private void embedXmlFile(PDDocument document, String xmlContent, String xmlFilename) throws Exception {
        PDDocumentCatalog catalog = document.getDocumentCatalog();
        byte[] xmlBytes = xmlContent.getBytes(StandardCharsets.UTF_8);
        PDEmbeddedFile embeddedFile = new PDEmbeddedFile(document, new ByteArrayInputStream(xmlBytes));
        embeddedFile.setSubtype(MIME_TYPE_XML);
        embeddedFile.setSize(xmlBytes.length);
        embeddedFile.setCreationDate(new GregorianCalendar());
        embeddedFile.setModDate(new GregorianCalendar());

        PDComplexFileSpecification fileSpec = new PDComplexFileSpecification();
        fileSpec.setFile(xmlFilename);
        fileSpec.setFileUnicode(xmlFilename);
        fileSpec.setEmbeddedFile(embeddedFile);
        fileSpec.setEmbeddedFileUnicode(embeddedFile);
        fileSpec.getCOSObject().setName(COSName.getPDFName("AFRelationship"), AFRelationship_SOURCE);

        PDEmbeddedFilesNameTreeNode embeddedFilesTree = new PDEmbeddedFilesNameTreeNode();
        embeddedFilesTree.setNames(Collections.singletonMap(xmlFilename, fileSpec));

        PDDocumentNameDictionary nameDictionary = catalog.getNames();
        if (nameDictionary == null) {
            nameDictionary = new PDDocumentNameDictionary(catalog);
            catalog.setNames(nameDictionary);
        }
        nameDictionary.setEmbeddedFiles(embeddedFilesTree);
        catalog.getCOSObject().setItem(COSName.getPDFName("AF"), fileSpec);
        log.debug("Embedded XML file: {} ({} bytes)", xmlFilename, xmlBytes.length);
    }

    public static class PdfConversionException extends Exception {
        public PdfConversionException(String message) { super(message); }
        public PdfConversionException(String message, Throwable cause) { super(message, cause); }
    }
}
```

- [ ] **Step 3: Create `ThaiAmountWordsConverter`** (identical logic to taxinvoice version, different package)

Create `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/pdf/ThaiAmountWordsConverter.java`:

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.pdf;

import java.math.BigDecimal;
import java.math.RoundingMode;

public final class ThaiAmountWordsConverter {

    private static final String[] DIGITS =
        {"ศูนย์", "หนึ่ง", "สอง", "สาม", "สี่", "ห้า", "หก", "เจ็ด", "แปด", "เก้า"};

    private ThaiAmountWordsConverter() {}

    public static String toWords(BigDecimal amount) {
        if (amount == null || amount.compareTo(BigDecimal.ZERO) < 0) {
            throw new IllegalArgumentException("Amount must be non-negative");
        }
        amount = amount.setScale(2, RoundingMode.HALF_UP);
        long totalSatang = amount.movePointRight(2).longValue();
        long baht   = totalSatang / 100;
        long satang = totalSatang % 100;

        StringBuilder result = new StringBuilder(numberToThai(baht));
        if (satang == 0) {
            result.append("บาทถ้วน");
        } else {
            result.append("บาท");
            result.append(numberToThai(satang));
            result.append("สตางค์");
        }
        return result.toString();
    }

    private static String numberToThai(long n) {
        if (n == 0) return "ศูนย์";

        StringBuilder sb = new StringBuilder();

        if (n >= 1_000_000) {
            sb.append(numberToThai(n / 1_000_000));
            sb.append("ล้าน");
            n %= 1_000_000;
            if (n == 0) return sb.toString();
        }

        boolean hasTens = (n % 100) >= 10;

        int[] place   = {100_000, 10_000, 1_000, 100, 10, 1};
        String[] unit = {"แสน",   "หมื่น", "พัน", "ร้อย", "สิบ", ""};

        for (int i = 0; i < place.length; i++) {
            int digit = (int) (n / place[i]);
            n %= place[i];
            if (digit == 0) continue;

            if (i == 4) {
                if (digit == 1)      sb.append("สิบ");
                else if (digit == 2) sb.append("ยี่สิบ");
                else                 sb.append(DIGITS[digit]).append("สิบ");
            } else if (i == 5) {
                if (digit == 1 && hasTens) sb.append("เอ็ด");
                else                        sb.append(DIGITS[digit]);
            } else {
                sb.append(DIGITS[digit]).append(unit[i]);
            }
        }
        return sb.toString();
    }
}
```

- [ ] **Step 4: Create `AbbreviatedTaxInvoicePdfGenerationServiceImpl`**

Create `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/pdf/AbbreviatedTaxInvoicePdfGenerationServiceImpl.java`:

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.pdf;

import com.wpanther.abbreviatedtaxinvoice.pdf.domain.service.AbbreviatedTaxInvoicePdfGenerationService;
import com.wpanther.abbreviatedtaxinvoice.pdf.domain.service.AbbreviatedTaxInvoicePdfGenerationService.AbbreviatedTaxInvoicePdfGenerationException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.xml.sax.InputSource;

import javax.xml.XMLConstants;
import javax.xml.namespace.NamespaceContext;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.StringReader;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

@Service
@Slf4j
public class AbbreviatedTaxInvoicePdfGenerationServiceImpl implements AbbreviatedTaxInvoicePdfGenerationService {

    private static final String RSM_NS =
        "urn:etda:uncefact:data:standard:AbbreviatedTaxInvoice_CrossIndustryInvoice:2";
    private static final String RAM_NS =
        "urn:etda:uncefact:data:standard:AbbreviatedTaxInvoice_ReusableAggregateBusinessInformationEntity:2";
    private static final String GRAND_TOTAL_XPATH =
        "/rsm:AbbreviatedTaxInvoice_CrossIndustryInvoice" +
        "/rsm:SupplyChainTradeTransaction" +
        "/ram:ApplicableHeaderTradeSettlement" +
        "/ram:SpecifiedTradeSettlementHeaderMonetarySummation" +
        "/ram:GrandTotalAmount";

    private static final NamespaceContext NS_CONTEXT = new NamespaceContext() {
        @Override
        public String getNamespaceURI(String prefix) {
            return switch (prefix) {
                case "rsm" -> RSM_NS;
                case "ram" -> RAM_NS;
                default    -> XMLConstants.NULL_NS_URI;
            };
        }
        @Override public String getPrefix(String ns) { return null; }
        @Override public Iterator<String> getPrefixes(String ns) { return Collections.emptyIterator(); }
    };

    private final FopAbbreviatedTaxInvoicePdfGenerator fopPdfGenerator;
    private final PdfA3Converter pdfA3Converter;

    public AbbreviatedTaxInvoicePdfGenerationServiceImpl(
            FopAbbreviatedTaxInvoicePdfGenerator fopPdfGenerator,
            PdfA3Converter pdfA3Converter) {
        this.fopPdfGenerator = fopPdfGenerator;
        this.pdfA3Converter  = pdfA3Converter;
    }

    @Override
    public byte[] generatePdf(String abbreviatedTaxInvoiceNumber, String signedXml)
            throws AbbreviatedTaxInvoicePdfGenerationException {

        log.info("Starting PDF generation for abbreviated tax invoice: {}", abbreviatedTaxInvoiceNumber);

        if (signedXml == null || signedXml.isBlank()) {
            throw new AbbreviatedTaxInvoicePdfGenerationException(
                "signedXml is null or blank for abbreviated tax invoice: " + abbreviatedTaxInvoiceNumber);
        }

        try {
            BigDecimal grandTotal  = extractGrandTotal(signedXml, abbreviatedTaxInvoiceNumber);
            String amountInWords   = ThaiAmountWordsConverter.toWords(grandTotal);
            log.debug("Grand total {} → amountInWords: {}", grandTotal, amountInWords);

            Map<String, Object> params = Map.of("amountInWords", amountInWords);
            byte[] basePdf = fopPdfGenerator.generatePdf(signedXml, params);
            log.debug("Generated base PDF: {} bytes", basePdf.length);

            String xmlFilename = "abbreviated-tax-invoice-" + abbreviatedTaxInvoiceNumber + ".xml";
            byte[] pdfA3 = pdfA3Converter.convertToPdfA3(
                    basePdf, signedXml, xmlFilename, abbreviatedTaxInvoiceNumber);
            log.info("Generated PDF/A-3 for abbreviated tax invoice {}: {} bytes",
                    abbreviatedTaxInvoiceNumber, pdfA3.length);
            return pdfA3;

        } catch (FopAbbreviatedTaxInvoicePdfGenerator.PdfGenerationException e) {
            log.error("FOP PDF generation failed for abbreviated tax invoice: {}",
                    abbreviatedTaxInvoiceNumber, e);
            throw new AbbreviatedTaxInvoicePdfGenerationException(
                    "PDF generation failed: " + e.getMessage(), e);
        } catch (PdfA3Converter.PdfConversionException e) {
            log.error("PDF/A-3 conversion failed for abbreviated tax invoice: {}",
                    abbreviatedTaxInvoiceNumber, e);
            throw new AbbreviatedTaxInvoicePdfGenerationException(
                    "PDF/A-3 conversion failed: " + e.getMessage(), e);
        } catch (AbbreviatedTaxInvoicePdfGenerationException e) {
            throw e;
        } catch (Exception e) {
            log.error("Unexpected error during PDF generation for abbreviated tax invoice: {}",
                    abbreviatedTaxInvoiceNumber, e);
            throw new AbbreviatedTaxInvoicePdfGenerationException(
                    "PDF generation failed: " + e.getMessage(), e);
        }
    }

    private BigDecimal extractGrandTotal(String signedXml, String abbreviatedTaxInvoiceNumber)
            throws AbbreviatedTaxInvoicePdfGenerationException {
        try {
            XPath xpath = XPathFactory.newInstance().newXPath();
            xpath.setNamespaceContext(NS_CONTEXT);
            String value = (String) xpath.evaluate(
                GRAND_TOTAL_XPATH,
                new InputSource(new StringReader(signedXml)),
                XPathConstants.STRING);
            if (value == null || value.isBlank()) {
                throw new AbbreviatedTaxInvoicePdfGenerationException(
                    "GrandTotalAmount not found in signed XML for abbreviated tax invoice: "
                    + abbreviatedTaxInvoiceNumber);
            }
            return new BigDecimal(value.trim());
        } catch (XPathExpressionException e) {
            throw new AbbreviatedTaxInvoicePdfGenerationException(
                "Failed to extract GrandTotalAmount from signed XML: " + e.getMessage(), e);
        } catch (NumberFormatException e) {
            throw new AbbreviatedTaxInvoicePdfGenerationException(
                "Invalid GrandTotalAmount in signed XML for abbreviated tax invoice "
                + abbreviatedTaxInvoiceNumber + ": " + e.getMessage(), e);
        }
    }
}
```

- [ ] **Step 5: Write `FopAbbreviatedTaxInvoicePdfGeneratorTest`**

Create `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/pdf/FopAbbreviatedTaxInvoicePdfGeneratorTest.java`:

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.pdf;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.*;

@DisplayName("FopAbbreviatedTaxInvoicePdfGenerator Unit Tests")
class FopAbbreviatedTaxInvoicePdfGeneratorTest {

    private static final String MINIMAL_SIGNED_XML =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
        "<rsm:AbbreviatedTaxInvoice_CrossIndustryInvoice " +
        "    xmlns:ram=\"urn:etda:uncefact:data:standard:AbbreviatedTaxInvoice_ReusableAggregateBusinessInformationEntity:2\"" +
        "    xmlns:rsm=\"urn:etda:uncefact:data:standard:AbbreviatedTaxInvoice_CrossIndustryInvoice:2\">" +
        "  <rsm:ExchangedDocument>" +
        "    <ram:ID>ATINV-TEST-001</ram:ID>" +
        "    <ram:Name>ใบกำกับภาษีอย่างย่อ</ram:Name>" +
        "    <ram:IssueDateTime>2024-01-15T00:00:00.0</ram:IssueDateTime>" +
        "  </rsm:ExchangedDocument>" +
        "  <rsm:SupplyChainTradeTransaction>" +
        "    <ram:ApplicableHeaderTradeAgreement>" +
        "      <ram:SellerTradeParty>" +
        "        <ram:Name>บริษัท ทดสอบ จำกัด</ram:Name>" +
        "        <ram:SpecifiedTaxRegistration><ram:ID>1234567890123</ram:ID></ram:SpecifiedTaxRegistration>" +
        "      </ram:SellerTradeParty>" +
        "    </ram:ApplicableHeaderTradeAgreement>" +
        "    <ram:ApplicableHeaderTradeDelivery/>" +
        "    <ram:ApplicableHeaderTradeSettlement>" +
        "      <ram:InvoiceCurrencyCode>THB</ram:InvoiceCurrencyCode>" +
        "      <ram:ApplicableTradeTax><ram:TypeCode>VAT</ram:TypeCode><ram:CalculatedRate>7</ram:CalculatedRate></ram:ApplicableTradeTax>" +
        "      <ram:SpecifiedTradeSettlementHeaderMonetarySummation>" +
        "        <ram:LineTotalAmount>1000</ram:LineTotalAmount>" +
        "        <ram:AllowanceTotalAmount>0</ram:AllowanceTotalAmount>" +
        "        <ram:TaxBasisTotalAmount>1000</ram:TaxBasisTotalAmount>" +
        "        <ram:TaxTotalAmount>70</ram:TaxTotalAmount>" +
        "        <ram:GrandTotalAmount>1070</ram:GrandTotalAmount>" +
        "      </ram:SpecifiedTradeSettlementHeaderMonetarySummation>" +
        "    </ram:ApplicableHeaderTradeSettlement>" +
        "    <ram:IncludedSupplyChainTradeLineItem>" +
        "      <ram:AssociatedDocumentLineDocument><ram:LineID>1</ram:LineID></ram:AssociatedDocumentLineDocument>" +
        "      <ram:SpecifiedTradeProduct><ram:ID>P001</ram:ID><ram:Name>สินค้าทดสอบ</ram:Name></ram:SpecifiedTradeProduct>" +
        "      <ram:SpecifiedLineTradeAgreement>" +
        "        <ram:GrossPriceProductTradePrice><ram:ChargeAmount>1000</ram:ChargeAmount></ram:GrossPriceProductTradePrice>" +
        "      </ram:SpecifiedLineTradeAgreement>" +
        "      <ram:SpecifiedLineTradeDelivery><ram:BilledQuantity unitCode=\"PIECE\">1</ram:BilledQuantity></ram:SpecifiedLineTradeDelivery>" +
        "      <ram:SpecifiedLineTradeSettlement>" +
        "        <ram:ApplicableTradeTax><ram:TypeCode>VAT</ram:TypeCode><ram:CalculatedRate>7</ram:CalculatedRate></ram:ApplicableTradeTax>" +
        "        <ram:SpecifiedTradeSettlementLineMonetarySummation><ram:NetLineTotalAmount>1000</ram:NetLineTotalAmount></ram:SpecifiedTradeSettlementLineMonetarySummation>" +
        "      </ram:SpecifiedLineTradeSettlement>" +
        "    </ram:IncludedSupplyChainTradeLineItem>" +
        "  </rsm:SupplyChainTradeTransaction>" +
        "</rsm:AbbreviatedTaxInvoice_CrossIndustryInvoice>";

    @Test
    @DisplayName("Constructor succeeds and compiles XSL template")
    void constructor_compilesTemplateSuccessfully() {
        assertThatCode(() ->
            new FopAbbreviatedTaxInvoicePdfGenerator(2, 52428800L, new SimpleMeterRegistry()))
            .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Constructor rejects maxConcurrentRenders < 1")
    void constructor_invalidMaxConcurrentRenders_throwsIllegalStateException() {
        assertThatThrownBy(() ->
            new FopAbbreviatedTaxInvoicePdfGenerator(0, 52428800L, new SimpleMeterRegistry()))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("max-concurrent-renders");
    }

    @Test
    @DisplayName("Constructor rejects maxPdfSizeBytes < 1")
    void constructor_invalidMaxPdfSizeBytes_throwsIllegalStateException() {
        assertThatThrownBy(() ->
            new FopAbbreviatedTaxInvoicePdfGenerator(1, 0L, new SimpleMeterRegistry()))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("max-pdf-size-bytes");
    }

    @Test
    @DisplayName("Semaphore is initialised with the configured permit count")
    void constructor_semaphorePermitsMatchConfiguration() throws Exception {
        FopAbbreviatedTaxInvoicePdfGenerator gen =
            new FopAbbreviatedTaxInvoicePdfGenerator(5, 52428800L, new SimpleMeterRegistry());
        Field f = FopAbbreviatedTaxInvoicePdfGenerator.class.getDeclaredField("renderSemaphore");
        f.setAccessible(true);
        Semaphore s = (Semaphore) f.get(gen);
        assertThat(s.availablePermits()).isEqualTo(5);
        assertThat(s.isFair()).isTrue();
    }

    @Test
    @DisplayName("checkFontAvailability() does not throw")
    void checkFontAvailability_doesNotThrow() {
        FopAbbreviatedTaxInvoicePdfGenerator gen =
            new FopAbbreviatedTaxInvoicePdfGenerator(1, 52428800L, new SimpleMeterRegistry());
        assertThatCode(() -> gen.checkFontAvailability()).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("PdfGenerationException 1-arg constructor carries message")
    void pdfGenerationException_messageOnlyConstructor() {
        var ex = new FopAbbreviatedTaxInvoicePdfGenerator.PdfGenerationException("FOP failed");
        assertThat(ex.getMessage()).isEqualTo("FOP failed");
        assertThat(ex.getCause()).isNull();
    }

    @Test
    @DisplayName("generatePdf(xml) on interrupted thread throws PdfGenerationException")
    void generatePdf_threadAlreadyInterrupted_throwsPdfGenerationException() {
        FopAbbreviatedTaxInvoicePdfGenerator gen =
            new FopAbbreviatedTaxInvoicePdfGenerator(1, 52428800L, new SimpleMeterRegistry());
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> gen.generatePdf(MINIMAL_SIGNED_XML))
                .isInstanceOf(FopAbbreviatedTaxInvoicePdfGenerator.PdfGenerationException.class)
                .hasMessageContaining("interrupted");
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    @DisplayName("Semaphore blocks callers when all permits are held")
    void generatePdf_semaphoreBlocksWhenAtCapacity() throws Exception {
        FopAbbreviatedTaxInvoicePdfGenerator gen =
            new FopAbbreviatedTaxInvoicePdfGenerator(1, 52428800L, new SimpleMeterRegistry());
        Field f = FopAbbreviatedTaxInvoicePdfGenerator.class.getDeclaredField("renderSemaphore");
        f.setAccessible(true);
        Semaphore sem = (Semaphore) f.get(gen);
        sem.acquire();

        ExecutorService exec = Executors.newSingleThreadExecutor();
        try {
            Future<?> future = exec.submit(() -> {
                try { gen.generatePdf(MINIMAL_SIGNED_XML); }
                catch (FopAbbreviatedTaxInvoicePdfGenerator.PdfGenerationException ignored) {}
            });
            assertThatThrownBy(() -> future.get(300, TimeUnit.MILLISECONDS))
                .isInstanceOf(TimeoutException.class);
            sem.release();
            future.get(5, TimeUnit.SECONDS);
        } finally {
            exec.shutdownNow();
        }
    }

    @Test
    @DisplayName("resolveBaseUri() returns a non-null absolute URI ending with '/'")
    void resolveBaseUri_returnsValidUri() throws Exception {
        FopAbbreviatedTaxInvoicePdfGenerator gen =
            new FopAbbreviatedTaxInvoicePdfGenerator(2, 52428800L, new SimpleMeterRegistry());
        Method method = FopAbbreviatedTaxInvoicePdfGenerator.class.getDeclaredMethod("resolveBaseUri");
        method.setAccessible(true);
        URI uri = (URI) method.invoke(gen);
        assertThat(uri).isNotNull();
        assertThat(uri.isAbsolute()).isTrue();
        assertThat(uri.toString()).endsWith("/");
    }

    @Test
    @DisplayName("Valid signed XML → returns non-empty PDF bytes starting with %PDF")
    void generatePdf_validSignedXml_returnsPdfBytes() throws Exception {
        FopAbbreviatedTaxInvoicePdfGenerator gen =
            new FopAbbreviatedTaxInvoicePdfGenerator(1, 52428800L, new SimpleMeterRegistry());
        byte[] result = gen.generatePdf(MINIMAL_SIGNED_XML,
            Map.of("amountInWords", "หนึ่งพันเจ็ดสิบบาทถ้วน"));
        assertThat(result).isNotEmpty();
        assertThat(new String(result, 0, 4, java.nio.charset.StandardCharsets.US_ASCII))
            .isEqualTo("%PDF");
    }

    @Test
    @DisplayName("generatePdf(xml) no-arg overload delegates successfully")
    void generatePdf_noParams_delegatesToParamsOverload() throws Exception {
        FopAbbreviatedTaxInvoicePdfGenerator gen =
            new FopAbbreviatedTaxInvoicePdfGenerator(1, 52428800L, new SimpleMeterRegistry());
        byte[] result = gen.generatePdf(MINIMAL_SIGNED_XML);
        assertThat(result).isNotEmpty();
    }

    @Test
    @DisplayName("Malformed XML → PdfGenerationException")
    void generatePdf_malformedXml_throwsPdfGenerationException() {
        FopAbbreviatedTaxInvoicePdfGenerator gen =
            new FopAbbreviatedTaxInvoicePdfGenerator(1, 52428800L, new SimpleMeterRegistry());
        assertThatThrownBy(() -> gen.generatePdf("this is not xml <<<"))
            .isInstanceOf(FopAbbreviatedTaxInvoicePdfGenerator.PdfGenerationException.class);
    }

    @Test
    @DisplayName("PDF exceeding max size → PdfGenerationException")
    void generatePdf_pdfExceedsMaxSize_throwsPdfGenerationException() {
        FopAbbreviatedTaxInvoicePdfGenerator gen =
            new FopAbbreviatedTaxInvoicePdfGenerator(1, 1L, new SimpleMeterRegistry());
        assertThatThrownBy(() -> gen.generatePdf(MINIMAL_SIGNED_XML, null))
            .isInstanceOf(FopAbbreviatedTaxInvoicePdfGenerator.PdfGenerationException.class)
            .hasMessageContaining("exceeds max allowed size");
    }
}
```

- [ ] **Step 6: Write `PdfA3ConverterTest`**

Create `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/pdf/PdfA3ConverterTest.java`:

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.pdf;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

@DisplayName("PdfA3Converter Unit Tests")
class PdfA3ConverterTest {

    private final PdfA3Converter converter = new PdfA3Converter("icc/sRGB.icc", new SimpleMeterRegistry());

    @Test
    @DisplayName("Constructor creates converter instance")
    void constructor_createsInstance() {
        assertThat(converter).isNotNull();
    }

    @Test
    @DisplayName("convertToPdfA3() throws PdfConversionException for null input")
    void testConvertToPdfA3_NullInput_Throws() {
        assertThatThrownBy(() ->
            converter.convertToPdfA3(null, "<xml/>", "test.xml", "ATINV-001"))
            .isInstanceOf(PdfA3Converter.PdfConversionException.class);
    }

    @Test
    @DisplayName("convertToPdfA3() throws PdfConversionException for empty PDF bytes")
    void testConvertToPdfA3_EmptyPdf_Throws() {
        assertThatThrownBy(() ->
            converter.convertToPdfA3(new byte[0], "<xml/>", "test.xml", "ATINV-001"))
            .isInstanceOf(PdfA3Converter.PdfConversionException.class);
    }

    @Test
    @DisplayName("PdfConversionException can be created with message")
    void testPdfConversionException_Message() {
        var exception = new PdfA3Converter.PdfConversionException("Test error");
        assertThat(exception).hasMessage("Test error");
    }

    @Test
    @DisplayName("PdfConversionException can be created with message and cause")
    void testPdfConversionException_MessageAndCause() {
        Throwable cause = new RuntimeException("Root cause");
        var exception = new PdfA3Converter.PdfConversionException("Test error", cause);
        assertThat(exception).hasMessage("Test error");
        assertThat(exception.getCause()).isSameAs(cause);
    }
}
```

- [ ] **Step 7: Run tests**

```bash
cd /home/wpanther/projects/etax/invoice-microservices/services/abbreviatedtaxinvoice-pdf-generation-service
mvn test -Dtest="FopAbbreviatedTaxInvoicePdfGeneratorTest,PdfA3ConverterTest" 2>&1 | tail -15
```

Expected: `BUILD SUCCESS`, all tests pass. The `FopAbbreviatedTaxInvoicePdfGenerator` test exercises the live XSL template (which will be written in a later task) — if the XSL is not yet present, only the `generatePdf` tests that call `renderPdf` will fail; constructor, semaphore, and interrupt tests will still pass. Run anyway and note which tests need the XSL template.

> **Note:** `generatePdf_validSignedXml_returnsPdfBytes` and related render tests require `abbreviatedtaxinvoice-direct.xsl` to be present (written in Task 12). If the XSL is absent, skip this test class for now and return after Task 12.

- [ ] **Step 8: Commit**

```bash
git add src/
git commit -m "feat: add PDF infrastructure — FOP generator, PDF/A-3 converter, amount words, service impl"
```

---

## Task 12: XSL-FO Template + FOP/ICC/Font Resources

**Files:**
- Create: `src/main/resources/xsl/abbreviatedtaxinvoice-direct.xsl`
- Copy from taxinvoice: `src/main/resources/fop/fop.xconf`
- Copy from taxinvoice: `src/main/resources/icc/sRGB.icc`
- Copy from taxinvoice: `src/main/resources/fonts/THSarabunNew.ttf` (and all font variants)
- Create: `src/test/resources/fop/fop.xconf` (simplified test config)

- [ ] **Step 1: Copy static resources from taxinvoice service**

```bash
TAXINVOICE_SRC=/home/wpanther/projects/etax/invoice-microservices/services/taxinvoice-pdf-generation-service/src/main/resources
THIS_SRC=/home/wpanther/projects/etax/invoice-microservices/services/abbreviatedtaxinvoice-pdf-generation-service/src/main/resources

mkdir -p "$THIS_SRC/fop" "$THIS_SRC/icc" "$THIS_SRC/fonts"
cp "$TAXINVOICE_SRC/fop/fop.xconf" "$THIS_SRC/fop/"
cp "$TAXINVOICE_SRC/icc/sRGB.icc" "$THIS_SRC/icc/"
cp "$TAXINVOICE_SRC/fonts/"*.ttf "$THIS_SRC/fonts/" 2>/dev/null || true
cp "$TAXINVOICE_SRC/fonts/"*.otf "$THIS_SRC/fonts/" 2>/dev/null || true
ls "$THIS_SRC/fop/" "$THIS_SRC/icc/" "$THIS_SRC/fonts/"
```

Expected: `fop.xconf`, `sRGB.icc`, and Thai font `.ttf` files listed.

- [ ] **Step 2: Create simplified test `fop.xconf`**

```bash
mkdir -p /home/wpanther/projects/etax/invoice-microservices/services/abbreviatedtaxinvoice-pdf-generation-service/src/test/resources/fop
```

Create `src/test/resources/fop/fop.xconf`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<fop version="1.0">
    <renderers>
        <renderer mime="application/pdf">
            <fonts>
                <auto-detect/>
            </fonts>
        </renderer>
    </renderers>
</fop>
```

- [ ] **Step 3: Create `abbreviatedtaxinvoice-direct.xsl`**

Create `src/main/resources/xsl/abbreviatedtaxinvoice-direct.xsl`. This is adapted from `taxinvoice-direct.xsl` with:
1. Namespace URIs changed to AbbreviatedTaxInvoice variants
2. Root match changed to `AbbreviatedTaxInvoice_CrossIndustryInvoice`
3. Document title changed to Thai abbreviated tax invoice wording
4. Party panel changed to full-width seller only (no buyer column)
5. `$buyer` variable removed

First read the original XSL to see its full content, then adapt:

```bash
wc -l /home/wpanther/projects/etax/invoice-microservices/services/taxinvoice-pdf-generation-service/src/main/resources/xsl/taxinvoice-direct.xsl
```

Then create the adapted XSL. The key structural difference is the party section changes from a 2-column (seller left, buyer right) table to a single full-width seller block:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0"
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns:fo="http://www.w3.org/1999/XSL/Format"
  xmlns:rsm="urn:etda:uncefact:data:standard:AbbreviatedTaxInvoice_CrossIndustryInvoice:2"
  xmlns:ram="urn:etda:uncefact:data:standard:AbbreviatedTaxInvoice_ReusableAggregateBusinessInformationEntity:2">

  <xsl:param name="amountInWords"/>

  <xsl:template match="/rsm:AbbreviatedTaxInvoice_CrossIndustryInvoice">

    <xsl:variable name="header"       select="rsm:ExchangedDocument"/>
    <xsl:variable name="transaction"  select="rsm:SupplyChainTradeTransaction"/>
    <xsl:variable name="agreement"    select="$transaction/ram:ApplicableHeaderTradeAgreement"/>
    <xsl:variable name="settlement"   select="$transaction/ram:ApplicableHeaderTradeSettlement"/>
    <xsl:variable name="seller"       select="$agreement/ram:SellerTradeParty"/>
    <xsl:variable name="summary"      select="$settlement/ram:SpecifiedTradeSettlementHeaderMonetarySummation"/>

    <fo:root>
      <fo:layout-master-set>
        <fo:simple-page-master master-name="A4"
            page-height="297mm" page-width="210mm"
            margin-top="10mm" margin-bottom="10mm"
            margin-left="15mm" margin-right="15mm">
          <fo:region-body margin-top="35mm" margin-bottom="10mm"/>
          <fo:region-before extent="35mm"/>
        </fo:simple-page-master>
      </fo:layout-master-set>

      <fo:page-sequence master-reference="A4">
        <!-- Header with document title -->
        <fo:static-content flow-name="xsl-region-before">
          <fo:block-container absolute-position="fixed" top="10mm" left="15mm" right="15mm">
            <fo:block text-align="center" font-size="16pt" font-weight="bold"
                font-family="THSarabunNew,serif">
              ใบเสร็จรับเงิน/ใบกำกับภาษีอย่างย่อ
            </fo:block>
            <fo:block text-align="center" font-size="12pt"
                font-family="THSarabunNew,serif" color="#444444">
              ABBREVIATED TAX INVOICE
            </fo:block>
            <fo:block text-align="center" font-size="9pt"
                font-family="THSarabunNew,serif" color="#666666">
              e-Tax Abbreviated Tax Invoice / ใบกำกับภาษีอย่างย่อ
            </fo:block>
          </fo:block-container>
        </fo:static-content>

        <fo:flow flow-name="xsl-region-body">

          <!-- Document number and date row -->
          <fo:table table-layout="fixed" width="100%" margin-bottom="4mm">
            <fo:table-column column-width="50%"/>
            <fo:table-column column-width="50%"/>
            <fo:table-body>
              <fo:table-row>
                <fo:table-cell>
                  <fo:block font-family="THSarabunNew,serif" font-size="10pt">
                    <fo:inline font-weight="bold">เลขที่ / No.: </fo:inline>
                    <xsl:value-of select="$header/ram:ID"/>
                  </fo:block>
                </fo:table-cell>
                <fo:table-cell text-align="right">
                  <fo:block font-family="THSarabunNew,serif" font-size="10pt">
                    <fo:inline font-weight="bold">วันที่ / Date: </fo:inline>
                    <xsl:value-of select="substring($header/ram:IssueDateTime, 1, 10)"/>
                  </fo:block>
                </fo:table-cell>
              </fo:table-row>
            </fo:table-body>
          </fo:table>

          <!-- Full-width Seller party box -->
          <fo:block-container border="0.5pt solid #999999" padding="3mm" margin-bottom="4mm">
            <fo:block font-family="THSarabunNew,serif" font-size="9pt"
                font-weight="bold" color="#555555" margin-bottom="1mm">
              ผู้ขาย / SELLER
            </fo:block>
            <fo:block font-family="THSarabunNew,serif" font-size="10pt" font-weight="bold">
              <xsl:value-of select="$seller/ram:Name"/>
            </fo:block>
            <xsl:if test="$seller/ram:PostalTradeAddress">
              <fo:block font-family="THSarabunNew,serif" font-size="9pt">
                <xsl:value-of select="$seller/ram:PostalTradeAddress/ram:LineOne"/>
                <xsl:if test="$seller/ram:PostalTradeAddress/ram:LineTwo">
                  <xsl:text> </xsl:text>
                  <xsl:value-of select="$seller/ram:PostalTradeAddress/ram:LineTwo"/>
                </xsl:if>
              </fo:block>
              <fo:block font-family="THSarabunNew,serif" font-size="9pt">
                <xsl:value-of select="$seller/ram:PostalTradeAddress/ram:CityName"/>
                <xsl:text> </xsl:text>
                <xsl:value-of select="$seller/ram:PostalTradeAddress/ram:PostcodeCode"/>
              </fo:block>
            </xsl:if>
            <fo:block font-family="THSarabunNew,serif" font-size="9pt">
              <fo:inline font-weight="bold">เลขประจำตัวผู้เสียภาษี / Tax ID: </fo:inline>
              <xsl:value-of select="$seller/ram:SpecifiedTaxRegistration/ram:ID"/>
            </fo:block>
          </fo:block-container>

          <!-- Line items table -->
          <fo:table table-layout="fixed" width="100%"
              border="0.5pt solid #cccccc" margin-bottom="3mm">
            <fo:table-column column-width="8%"/>
            <fo:table-column column-width="40%"/>
            <fo:table-column column-width="12%"/>
            <fo:table-column column-width="20%"/>
            <fo:table-column column-width="20%"/>
            <fo:table-header>
              <fo:table-row background-color="#f0f0f0">
                <fo:table-cell border="0.5pt solid #cccccc" padding="2mm">
                  <fo:block font-family="THSarabunNew,serif" font-size="9pt"
                      font-weight="bold" text-align="center">#</fo:block>
                </fo:table-cell>
                <fo:table-cell border="0.5pt solid #cccccc" padding="2mm">
                  <fo:block font-family="THSarabunNew,serif" font-size="9pt"
                      font-weight="bold">รายการ / Description</fo:block>
                </fo:table-cell>
                <fo:table-cell border="0.5pt solid #cccccc" padding="2mm">
                  <fo:block font-family="THSarabunNew,serif" font-size="9pt"
                      font-weight="bold" text-align="center">จำนวน / Qty</fo:block>
                </fo:table-cell>
                <fo:table-cell border="0.5pt solid #cccccc" padding="2mm">
                  <fo:block font-family="THSarabunNew,serif" font-size="9pt"
                      font-weight="bold" text-align="right">ราคาต่อหน่วย / Unit Price</fo:block>
                </fo:table-cell>
                <fo:table-cell border="0.5pt solid #cccccc" padding="2mm">
                  <fo:block font-family="THSarabunNew,serif" font-size="9pt"
                      font-weight="bold" text-align="right">จำนวนเงิน / Amount</fo:block>
                </fo:table-cell>
              </fo:table-row>
            </fo:table-header>
            <fo:table-body>
              <xsl:for-each select="$transaction/ram:IncludedSupplyChainTradeLineItem">
                <fo:table-row>
                  <fo:table-cell border="0.5pt solid #cccccc" padding="2mm">
                    <fo:block font-family="THSarabunNew,serif" font-size="9pt" text-align="center">
                      <xsl:value-of select="ram:AssociatedDocumentLineDocument/ram:LineID"/>
                    </fo:block>
                  </fo:table-cell>
                  <fo:table-cell border="0.5pt solid #cccccc" padding="2mm">
                    <fo:block font-family="THSarabunNew,serif" font-size="9pt">
                      <xsl:value-of select="ram:SpecifiedTradeProduct/ram:Name"/>
                    </fo:block>
                  </fo:table-cell>
                  <fo:table-cell border="0.5pt solid #cccccc" padding="2mm">
                    <fo:block font-family="THSarabunNew,serif" font-size="9pt" text-align="center">
                      <xsl:value-of select="ram:SpecifiedLineTradeDelivery/ram:BilledQuantity"/>
                    </fo:block>
                  </fo:table-cell>
                  <fo:table-cell border="0.5pt solid #cccccc" padding="2mm">
                    <fo:block font-family="THSarabunNew,serif" font-size="9pt" text-align="right">
                      <xsl:value-of select="format-number(
                          ram:SpecifiedLineTradeAgreement/ram:GrossPriceProductTradePrice/ram:ChargeAmount,
                          '#,##0.00')"/>
                    </fo:block>
                  </fo:table-cell>
                  <fo:table-cell border="0.5pt solid #cccccc" padding="2mm">
                    <fo:block font-family="THSarabunNew,serif" font-size="9pt" text-align="right">
                      <xsl:value-of select="format-number(
                          ram:SpecifiedLineTradeSettlement/ram:SpecifiedTradeSettlementLineMonetarySummation/ram:NetLineTotalAmount,
                          '#,##0.00')"/>
                    </fo:block>
                  </fo:table-cell>
                </fo:table-row>
              </xsl:for-each>
            </fo:table-body>
          </fo:table>

          <!-- Monetary summary -->
          <fo:table table-layout="fixed" width="100%" margin-bottom="3mm">
            <fo:table-column column-width="60%"/>
            <fo:table-column column-width="40%"/>
            <fo:table-body>
              <fo:table-row>
                <fo:table-cell>
                  <fo:block font-family="THSarabunNew,serif" font-size="9pt">
                    <fo:inline font-weight="bold">จำนวนเงินรวม (ตัวอักษร) / Amount in words: </fo:inline>
                    <xsl:value-of select="$amountInWords"/>
                  </fo:block>
                </fo:table-cell>
                <fo:table-cell>
                  <fo:table table-layout="fixed" width="100%">
                    <fo:table-column column-width="55%"/>
                    <fo:table-column column-width="45%"/>
                    <fo:table-body>
                      <fo:table-row>
                        <fo:table-cell padding="1mm">
                          <fo:block font-family="THSarabunNew,serif" font-size="9pt">
                            รวมก่อนภาษี / Subtotal:
                          </fo:block>
                        </fo:table-cell>
                        <fo:table-cell padding="1mm">
                          <fo:block font-family="THSarabunNew,serif" font-size="9pt" text-align="right">
                            <xsl:value-of select="format-number($summary/ram:TaxBasisTotalAmount, '#,##0.00')"/>
                          </fo:block>
                        </fo:table-cell>
                      </fo:table-row>
                      <fo:table-row>
                        <fo:table-cell padding="1mm">
                          <fo:block font-family="THSarabunNew,serif" font-size="9pt">
                            ภาษีมูลค่าเพิ่ม / VAT:
                          </fo:block>
                        </fo:table-cell>
                        <fo:table-cell padding="1mm">
                          <fo:block font-family="THSarabunNew,serif" font-size="9pt" text-align="right">
                            <xsl:value-of select="format-number($summary/ram:TaxTotalAmount, '#,##0.00')"/>
                          </fo:block>
                        </fo:table-cell>
                      </fo:table-row>
                      <fo:table-row>
                        <fo:table-cell padding="1mm" border-top="0.5pt solid #000000">
                          <fo:block font-family="THSarabunNew,serif" font-size="10pt" font-weight="bold">
                            ยอดรวมสุทธิ / Grand Total:
                          </fo:block>
                        </fo:table-cell>
                        <fo:table-cell padding="1mm" border-top="0.5pt solid #000000">
                          <fo:block font-family="THSarabunNew,serif" font-size="10pt"
                              font-weight="bold" text-align="right">
                            <xsl:value-of select="format-number($summary/ram:GrandTotalAmount, '#,##0.00')"/>
                          </fo:block>
                        </fo:table-cell>
                      </fo:table-row>
                    </fo:table-body>
                  </fo:table>
                </fo:table-cell>
              </fo:table-row>
            </fo:table-body>
          </fo:table>

        </fo:flow>
      </fo:page-sequence>
    </fo:root>
  </xsl:template>

</xsl:stylesheet>
```

- [ ] **Step 4: Run FOP generator tests with XSL now present**

```bash
mvn test -Dtest="FopAbbreviatedTaxInvoicePdfGeneratorTest,PdfA3ConverterTest" 2>&1 | tail -15
```

Expected: `BUILD SUCCESS`, all tests pass including `generatePdf_validSignedXml_returnsPdfBytes`.

- [ ] **Step 5: Commit**

```bash
git add src/
git commit -m "feat: add XSL-FO template for abbreviated tax invoice and static FOP/ICC/font resources"
```

---

## Task 13: MinIO Storage Adapter + Cleanup Service

**Files:**
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/storage/MinioStorageAdapter.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/storage/MinioCleanupService.java`
- Test: `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/storage/MinioStorageAdapterTest.java`
- Test: `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/storage/MinioCleanupServiceTest.java`

- [ ] **Step 1: Create `MinioStorageAdapter`**

Create `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/storage/MinioStorageAdapter.java`:

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.storage;

import com.wpanther.abbreviatedtaxinvoice.pdf.application.port.out.PdfStoragePort;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.net.URI;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Component
@Slf4j
public class MinioStorageAdapter implements PdfStoragePort {

    private final S3Client s3Client;
    private final String bucketName;
    private final String baseUrl;
    private final Timer storeTimer;
    private final Timer deleteTimer;

    public MinioStorageAdapter(
            S3Client s3Client,
            @Value("${app.minio.bucket-name}") String bucketName,
            @Value("${app.minio.base-url}") String baseUrl,
            MeterRegistry meterRegistry) {
        try {
            URI uri = URI.create(baseUrl);
            if (uri.getScheme() == null || uri.getHost() == null) {
                throw new IllegalArgumentException("not an absolute URL");
            }
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("app.minio.base-url is not a valid absolute URL: " + baseUrl, e);
        }
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.storeTimer = meterRegistry.timer("pdf.minio.store", "bucket", bucketName);
        this.deleteTimer = meterRegistry.timer("pdf.minio.delete", "bucket", bucketName);
    }

    @Override
    @CircuitBreaker(name = "minio")
    public String store(String abbreviatedTaxInvoiceNumber, byte[] pdfBytes) {
        return storeTimer.record(() -> doStore(abbreviatedTaxInvoiceNumber, pdfBytes));
    }

    @Override
    @CircuitBreaker(name = "minio")
    public void delete(String s3Key) {
        deleteTimer.record(() -> doDelete(s3Key));
    }

    @Override
    public String resolveUrl(String s3Key) {
        return baseUrl + "/" + s3Key;
    }

    private String doStore(String abbreviatedTaxInvoiceNumber, byte[] pdfBytes) {
        LocalDate now = LocalDate.now();
        String safeName = sanitizeFilename(abbreviatedTaxInvoiceNumber);
        String fileName = String.format("abbreviated-tax-invoice-%s-%s.pdf", safeName, UUID.randomUUID());
        String s3Key = String.format("%04d/%02d/%02d/%s",
                now.getYear(), now.getMonthValue(), now.getDayOfMonth(), fileName);

        PutObjectRequest put = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .contentType("application/pdf")
                .contentLength((long) pdfBytes.length)
                .build();

        s3Client.putObject(put, RequestBody.fromBytes(pdfBytes));
        log.debug("Uploaded PDF to MinIO: bucket={}, key={}", bucketName, s3Key);
        return s3Key;
    }

    private String sanitizeFilename(String rawNumber) {
        return rawNumber.replaceAll("[\\x00-\\x1F\\x7F\\\\\"'*?<>|:&; @]", "_")
                .replaceAll("\\s+", "_");
    }

    private void doDelete(String s3Key) {
        s3Client.deleteObject(DeleteObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .build());
        log.info("Deleted PDF from MinIO: bucket={}, key={}", bucketName, s3Key);
    }

    public List<String> listAllPdfs() {
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .build();
        ListObjectsV2Response response = s3Client.listObjectsV2(request);
        return response.contents().stream()
                .map(S3Object::key)
                .collect(Collectors.toList());
    }

    public void deleteWithoutCircuitBreaker(String s3Key) {
        try {
            doDelete(s3Key);
        } catch (Exception e) {
            log.warn("Failed to delete orphaned PDF from MinIO: bucket={}, key={}, error={}",
                    bucketName, s3Key, e.getMessage());
        }
    }
}
```

- [ ] **Step 2: Create `MinioCleanupService`**

Create `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/storage/MinioCleanupService.java`:

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.storage;

import com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.persistence.JpaAbbreviatedTaxInvoicePdfDocumentRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;

@Component
@RequiredArgsConstructor
@Slf4j
@ConditionalOnProperty(name = "app.minio.cleanup.enabled", havingValue = "true", matchIfMissing = false)
public class MinioCleanupService {

    private final MinioStorageAdapter minioStorage;
    private final JpaAbbreviatedTaxInvoicePdfDocumentRepository repository;

    @Scheduled(cron = "${app.minio.cleanup.cron:0 0 2 * * ?}")
    public void cleanupOrphanedPdfs() {
        log.info("Starting orphaned PDF cleanup job");
        try {
            List<String> minioKeys = minioStorage.listAllPdfs();
            log.debug("Found {} PDF objects in MinIO", minioKeys.size());

            Set<String> databaseKeys = repository.findAllDocumentPaths();
            log.debug("Found {} document paths in database", databaseKeys.size());

            List<String> orphanedKeys = minioKeys.stream()
                    .filter(key -> !databaseKeys.contains(key))
                    .toList();

            if (orphanedKeys.isEmpty()) {
                log.info("No orphaned PDFs found");
                return;
            }

            log.warn("Found {} orphaned PDF(s) to delete: {}", orphanedKeys.size(), orphanedKeys);
            int deletedCount = 0;
            for (String key : orphanedKeys) {
                minioStorage.deleteWithoutCircuitBreaker(key);
                deletedCount++;
            }
            log.info("Orphaned PDF cleanup completed: {} of {} objects deleted",
                    deletedCount, orphanedKeys.size());
        } catch (Exception e) {
            log.error("Orphaned PDF cleanup job failed: {}", e.getMessage(), e);
        }
    }
}
```

- [ ] **Step 3: Write `MinioStorageAdapterTest`**

Create `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/storage/MinioStorageAdapterTest.java`:

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.storage;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MinioStorageAdapterTest {

    @Mock
    private S3Client s3Client;

    private MinioStorageAdapter adapter;

    @BeforeEach
    void setUp() {
        adapter = new MinioStorageAdapter(s3Client, "abbreviatedtaxinvoices",
                "http://localhost:9000/abbreviatedtaxinvoices", new SimpleMeterRegistry());
    }

    @Test
    void store_uploadsAndReturnsS3Key() {
        when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenReturn(PutObjectResponse.builder().build());

        String key = adapter.store("ATINV-001", new byte[]{1, 2, 3});

        assertThat(key).matches("\\d{4}/\\d{2}/\\d{2}/abbreviated-tax-invoice-ATINV-001-.+\\.pdf");
        verify(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }

    @Test
    void resolveUrl_prependsBaseUrl() {
        String url = adapter.resolveUrl("2024/01/15/abbreviated-tax-invoice-001.pdf");
        assertThat(url).isEqualTo(
                "http://localhost:9000/abbreviatedtaxinvoices/2024/01/15/abbreviated-tax-invoice-001.pdf");
    }

    @Test
    void delete_callsS3DeleteObject() {
        when(s3Client.deleteObject(any(DeleteObjectRequest.class)))
                .thenReturn(DeleteObjectResponse.builder().build());

        assertThatNoException().isThrownBy(() -> adapter.delete("2024/01/15/file.pdf"));
        verify(s3Client).deleteObject(any(DeleteObjectRequest.class));
    }

    @Test
    void delete_s3Failure_throwsException() {
        when(s3Client.deleteObject(any(DeleteObjectRequest.class)))
                .thenThrow(new RuntimeException("S3 error"));

        assertThatThrownBy(() -> adapter.delete("bad-key"))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void store_preservesThaiCharacters() {
        when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenReturn(PutObjectResponse.builder().build());

        String key = adapter.store("ATINV-ไทย-001", new byte[]{1, 2, 3});

        assertThat(key).contains("abbreviated-tax-invoice-ATINV-ไทย-001-");
        verify(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }

    @Test
    void store_sanitizesProblematicCharacters() {
        when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenReturn(PutObjectResponse.builder().build());

        String key = adapter.store("ATINV:test<>|001", new byte[]{1, 2, 3});

        assertThat(key).contains("abbreviated-tax-invoice-ATINV_test___001-");
        verify(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }
}
```

- [ ] **Step 4: Write `MinioCleanupServiceTest`**

Create `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/storage/MinioCleanupServiceTest.java`:

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.storage;

import com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.persistence.JpaAbbreviatedTaxInvoicePdfDocumentRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("MinioCleanupService Unit Tests")
class MinioCleanupServiceTest {

    @Mock
    private MinioStorageAdapter minioStorage;

    @Mock
    private JpaAbbreviatedTaxInvoicePdfDocumentRepository repository;

    private MinioCleanupService getService() {
        try {
            return MinioCleanupService.class
                    .getDeclaredConstructor(MinioStorageAdapter.class,
                            JpaAbbreviatedTaxInvoicePdfDocumentRepository.class)
                    .newInstance(minioStorage, repository);
        } catch (Exception e) {
            throw new RuntimeException("Failed to instantiate MinioCleanupService", e);
        }
    }

    @Test
    @DisplayName("cleanupOrphanedPdfs() deletes objects not in database")
    void testCleanupOrphanedPdfs() {
        List<String> minioKeys = List.of(
                "2024/01/15/abbreviated-tax-invoice-ATINV-001-abc.pdf",
                "2024/01/15/abbreviated-tax-invoice-ATINV-002-def.pdf",
                "2024/01/15/abbreviated-tax-invoice-ATINV-003-orphan.pdf");
        Set<String> databaseKeys = Set.of(
                "2024/01/15/abbreviated-tax-invoice-ATINV-001-abc.pdf",
                "2024/01/15/abbreviated-tax-invoice-ATINV-002-def.pdf");

        when(minioStorage.listAllPdfs()).thenReturn(minioKeys);
        when(repository.findAllDocumentPaths()).thenReturn(databaseKeys);

        getService().cleanupOrphanedPdfs();

        verify(minioStorage).deleteWithoutCircuitBreaker(
                "2024/01/15/abbreviated-tax-invoice-ATINV-003-orphan.pdf");
        verify(minioStorage, never()).deleteWithoutCircuitBreaker(
                "2024/01/15/abbreviated-tax-invoice-ATINV-001-abc.pdf");
    }

    @Test
    @DisplayName("cleanupOrphanedPdfs() does nothing when no orphaned objects exist")
    void testCleanupOrphanedPdfs_NoOrphans() {
        List<String> minioKeys = List.of("2024/01/15/abbreviated-tax-invoice-ATINV-001-abc.pdf");
        Set<String> databaseKeys = Set.of("2024/01/15/abbreviated-tax-invoice-ATINV-001-abc.pdf");

        when(minioStorage.listAllPdfs()).thenReturn(minioKeys);
        when(repository.findAllDocumentPaths()).thenReturn(databaseKeys);

        getService().cleanupOrphanedPdfs();

        verify(minioStorage, never()).deleteWithoutCircuitBreaker(anyString());
    }

    @Test
    @DisplayName("cleanupOrphanedPdfs() handles MinIO listing errors gracefully")
    void testCleanupOrphanedPdfs_ListingError() {
        when(minioStorage.listAllPdfs()).thenThrow(new RuntimeException("MinIO connection error"));

        assertDoesNotThrow(() -> getService().cleanupOrphanedPdfs());
        verify(repository, never()).findAllDocumentPaths();
    }

    @Test
    @DisplayName("cleanupOrphanedPdfs() handles database query errors gracefully")
    void testCleanupOrphanedPdfs_DatabaseError() {
        when(minioStorage.listAllPdfs()).thenReturn(
                List.of("2024/01/15/abbreviated-tax-invoice-ATINV-001-abc.pdf"));
        when(repository.findAllDocumentPaths()).thenThrow(new RuntimeException("DB error"));

        assertDoesNotThrow(() -> getService().cleanupOrphanedPdfs());
    }

    @Test
    @DisplayName("cleanupOrphanedPdfs() deletes all objects when database is empty")
    void testCleanupOrphanedPdfs_AllOrphans() {
        List<String> minioKeys = List.of(
                "2024/01/15/abbreviated-tax-invoice-ATINV-001-abc.pdf",
                "2024/01/15/abbreviated-tax-invoice-ATINV-002-def.pdf");

        when(minioStorage.listAllPdfs()).thenReturn(minioKeys);
        when(repository.findAllDocumentPaths()).thenReturn(Set.of());

        getService().cleanupOrphanedPdfs();

        verify(minioStorage).deleteWithoutCircuitBreaker(
                "2024/01/15/abbreviated-tax-invoice-ATINV-001-abc.pdf");
        verify(minioStorage).deleteWithoutCircuitBreaker(
                "2024/01/15/abbreviated-tax-invoice-ATINV-002-def.pdf");
    }
}
```

- [ ] **Step 5: Run storage tests**

```bash
mvn test -Dtest="MinioStorageAdapterTest,MinioCleanupServiceTest" 2>&1 | tail -10
```

Expected: `BUILD SUCCESS`, all tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/
git commit -m "feat: add MinIO storage adapter and cleanup service"
```

---

## Task 14: REST Client + Application Services (DocumentService + SagaCommandHandler)

**Files:**
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/client/RestTemplateSignedXmlFetcher.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/application/service/AbbreviatedTaxInvoicePdfDocumentService.java`
- Create: `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/application/service/SagaCommandHandler.java`
- Test: `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/client/RestTemplateSignedXmlFetcherTest.java`
- Test: `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/application/service/SagaCommandHandlerTest.java`
- Test: `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/application/service/AbbreviatedTaxInvoicePdfDocumentServiceTest.java`

- [ ] **Step 1: Create `RestTemplateSignedXmlFetcher`**

Create `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/client/RestTemplateSignedXmlFetcher.java`:

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.client;

import com.wpanther.abbreviatedtaxinvoice.pdf.application.port.out.SignedXmlFetchPort;
import com.wpanther.abbreviatedtaxinvoice.pdf.application.port.out.SignedXmlFetchPort.SignedXmlFetchException;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
@RequiredArgsConstructor
@Slf4j
public class RestTemplateSignedXmlFetcher implements SignedXmlFetchPort {

    private final RestTemplate restTemplate;

    @Override
    @CircuitBreaker(name = "signedXmlFetch", fallbackMethod = "fallbackOnFailure")
    public String fetch(String signedXmlUrl) {
        log.debug("Fetching signed XML from {}", signedXmlUrl);
        String response = restTemplate.getForObject(signedXmlUrl, String.class);
        if (response == null || response.isBlank()) {
            throw new IllegalStateException(
                    "Received null or empty signed XML response from: " + signedXmlUrl);
        }
        log.debug("Successfully fetched signed XML, size: {} bytes", response.length());
        return response;
    }

    private String fallbackOnFailure(String signedXmlUrl, Throwable throwable) {
        throw new SignedXmlFetchException(
                "Circuit breaker 'signedXmlFetch' is OPEN — " +
                "document-storage-service is degraded. URL: " + signedXmlUrl, throwable);
    }
}
```

- [ ] **Step 2: Write `RestTemplateSignedXmlFetcherTest`**

Create `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/client/RestTemplateSignedXmlFetcherTest.java`:

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.client;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestTemplate;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.*;
import static org.springframework.test.web.client.response.MockRestResponseCreators.*;

class RestTemplateSignedXmlFetcherTest {

    private RestTemplate restTemplate;
    private MockRestServiceServer mockServer;
    private RestTemplateSignedXmlFetcher fetcher;

    @BeforeEach
    void setUp() {
        restTemplate = new RestTemplate();
        mockServer = MockRestServiceServer.createServer(restTemplate);
        fetcher = new RestTemplateSignedXmlFetcher(restTemplate);
    }

    @Test
    void fetch_success_returnsXml() {
        mockServer.expect(requestTo("http://minio/xml"))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("<abbreviatedInvoice/>", MediaType.APPLICATION_XML));

        String result = fetcher.fetch("http://minio/xml");

        assertThat(result).isEqualTo("<abbreviatedInvoice/>");
        mockServer.verify();
    }

    @Test
    void fetch_emptyResponse_throwsException() {
        mockServer.expect(requestTo("http://minio/empty"))
                .andRespond(withSuccess("", MediaType.APPLICATION_XML));

        assertThatThrownBy(() -> fetcher.fetch("http://minio/empty"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("empty");
    }

    @Test
    void fetch_serverError_throwsException() {
        mockServer.expect(requestTo("http://minio/error"))
                .andRespond(withServerError());

        assertThatThrownBy(() -> fetcher.fetch("http://minio/error"))
                .isInstanceOf(Exception.class);
    }
}
```

- [ ] **Step 3: Create `AbbreviatedTaxInvoicePdfDocumentService`**

Create `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/application/service/AbbreviatedTaxInvoicePdfDocumentService.java`:

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.application.service;

import com.wpanther.abbreviatedtaxinvoice.pdf.application.port.out.PdfEventPort;
import com.wpanther.abbreviatedtaxinvoice.pdf.application.port.out.SagaReplyPort;
import com.wpanther.abbreviatedtaxinvoice.pdf.domain.model.AbbreviatedTaxInvoicePdfDocument;
import com.wpanther.abbreviatedtaxinvoice.pdf.domain.repository.AbbreviatedTaxInvoicePdfDocumentRepository;
import com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.in.kafka.KafkaAbbreviatedTaxInvoiceCompensateCommand;
import com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.in.kafka.KafkaAbbreviatedTaxInvoiceProcessCommand;
import com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.messaging.AbbreviatedTaxInvoicePdfGeneratedEvent;
import com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.metrics.PdfGenerationMetrics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class AbbreviatedTaxInvoicePdfDocumentService {

    private final AbbreviatedTaxInvoicePdfDocumentRepository repository;
    private final PdfEventPort pdfEventPort;
    private final SagaReplyPort sagaReplyPort;
    private final PdfGenerationMetrics pdfGenerationMetrics;

    @Transactional(readOnly = true)
    public Optional<AbbreviatedTaxInvoicePdfDocument> findByAbbreviatedTaxInvoiceId(
            String abbreviatedTaxInvoiceId) {
        return repository.findByAbbreviatedTaxInvoiceId(abbreviatedTaxInvoiceId);
    }

    @Transactional
    public AbbreviatedTaxInvoicePdfDocument beginGeneration(
            String abbreviatedTaxInvoiceId, String abbreviatedTaxInvoiceNumber) {
        log.info("Initiating PDF generation for abbreviated tax invoice: {}", abbreviatedTaxInvoiceNumber);
        AbbreviatedTaxInvoicePdfDocument doc = AbbreviatedTaxInvoicePdfDocument.builder()
                .abbreviatedTaxInvoiceId(abbreviatedTaxInvoiceId)
                .abbreviatedTaxInvoiceNumber(abbreviatedTaxInvoiceNumber)
                .build();
        doc.startGeneration();
        return repository.save(doc);
    }

    @Transactional
    public AbbreviatedTaxInvoicePdfDocument replaceAndBeginGeneration(
            UUID existingId, int previousRetryCount,
            String abbreviatedTaxInvoiceId, String abbreviatedTaxInvoiceNumber) {
        log.info("Replacing document {} and re-starting generation for abbreviated tax invoice: {}",
                existingId, abbreviatedTaxInvoiceNumber);
        repository.deleteById(existingId);
        repository.flush();
        AbbreviatedTaxInvoicePdfDocument doc = AbbreviatedTaxInvoicePdfDocument.builder()
                .abbreviatedTaxInvoiceId(abbreviatedTaxInvoiceId)
                .abbreviatedTaxInvoiceNumber(abbreviatedTaxInvoiceNumber)
                .build();
        doc.startGeneration();
        doc.incrementRetryCountTo(previousRetryCount + 1);
        return repository.save(doc);
    }

    @Transactional
    public void completeGenerationAndPublish(UUID documentId, String s3Key, String fileUrl,
                                             long fileSize, int previousRetryCount,
                                             KafkaAbbreviatedTaxInvoiceProcessCommand command) {
        AbbreviatedTaxInvoicePdfDocument doc = requireDocument(documentId);
        doc.markCompleted(s3Key, fileUrl, fileSize);
        doc.markXmlEmbedded();
        applyRetryCount(doc, previousRetryCount);
        doc = repository.save(doc);

        pdfEventPort.publishPdfGenerated(buildGeneratedEvent(doc, command));
        sagaReplyPort.publishSuccess(
                command.getSagaId(), command.getSagaStep(), command.getCorrelationId(),
                doc.getDocumentUrl(), doc.getFileSize());

        log.info("Completed PDF generation for saga {} abbreviated tax invoice {}",
                command.getSagaId(), doc.getAbbreviatedTaxInvoiceNumber());
    }

    @Transactional
    public void failGenerationAndPublish(UUID documentId, String errorMessage,
                                         int previousRetryCount,
                                         KafkaAbbreviatedTaxInvoiceProcessCommand command) {
        String safeError = errorMessage != null ? errorMessage : "PDF generation failed";
        AbbreviatedTaxInvoicePdfDocument doc = requireDocument(documentId);
        doc.markFailed(safeError);
        applyRetryCount(doc, previousRetryCount);
        repository.save(doc);

        sagaReplyPort.publishFailure(
                command.getSagaId(), command.getSagaStep(), command.getCorrelationId(), safeError);

        log.warn("PDF generation failed for saga {} abbreviated tax invoice {}: {}",
                command.getSagaId(), doc.getAbbreviatedTaxInvoiceNumber(), safeError);
    }

    @Transactional
    public void deleteById(UUID documentId) {
        repository.deleteById(documentId);
        repository.flush();
    }

    @Transactional
    public void publishIdempotentSuccess(AbbreviatedTaxInvoicePdfDocument existing,
                                         KafkaAbbreviatedTaxInvoiceProcessCommand command) {
        pdfEventPort.publishPdfGenerated(buildGeneratedEvent(existing, command));
        sagaReplyPort.publishSuccess(
                command.getSagaId(), command.getSagaStep(), command.getCorrelationId(),
                existing.getDocumentUrl(), existing.getFileSize());
        log.warn("Abbreviated tax invoice PDF already generated for saga {} — re-publishing SUCCESS reply",
                command.getSagaId());
    }

    @Transactional
    public void publishRetryExhausted(KafkaAbbreviatedTaxInvoiceProcessCommand command) {
        pdfGenerationMetrics.recordRetryExhausted(
                command.getSagaId(), command.getDocumentId(), command.getDocumentNumber());
        sagaReplyPort.publishFailure(
                command.getSagaId(), command.getSagaStep(), command.getCorrelationId(),
                "Maximum retry attempts exceeded");
        log.error("Max retries exceeded for saga {} document {}",
                command.getSagaId(), command.getDocumentNumber());
    }

    @Transactional
    public void publishGenerationFailure(KafkaAbbreviatedTaxInvoiceProcessCommand command, String errorMessage) {
        sagaReplyPort.publishFailure(
                command.getSagaId(), command.getSagaStep(), command.getCorrelationId(), errorMessage);
    }

    @Transactional
    public void publishCompensated(KafkaAbbreviatedTaxInvoiceCompensateCommand command) {
        sagaReplyPort.publishCompensated(
                command.getSagaId(), command.getSagaStep(), command.getCorrelationId());
    }

    @Transactional
    public void publishCompensationFailure(KafkaAbbreviatedTaxInvoiceCompensateCommand command, String error) {
        sagaReplyPort.publishFailure(
                command.getSagaId(), command.getSagaStep(), command.getCorrelationId(), error);
    }

    private AbbreviatedTaxInvoicePdfDocument requireDocument(UUID documentId) {
        return repository.findById(documentId)
                .orElseThrow(() -> {
                    log.error("AbbreviatedTaxInvoicePdfDocument not found for id={}", documentId);
                    return new IllegalStateException(
                            "Expected abbreviated tax invoice PDF document is absent");
                });
    }

    private void applyRetryCount(AbbreviatedTaxInvoicePdfDocument doc, int previousRetryCount) {
        if (previousRetryCount < 0) return;
        doc.incrementRetryCountTo(previousRetryCount + 1);
    }

    private AbbreviatedTaxInvoicePdfGeneratedEvent buildGeneratedEvent(
            AbbreviatedTaxInvoicePdfDocument doc, KafkaAbbreviatedTaxInvoiceProcessCommand command) {
        return new AbbreviatedTaxInvoicePdfGeneratedEvent(
                command.getSagaId(),
                command.getDocumentId(),
                doc.getAbbreviatedTaxInvoiceNumber(),
                doc.getDocumentUrl(),
                doc.getFileSize(),
                doc.isXmlEmbedded(),
                command.getCorrelationId());
    }
}
```

- [ ] **Step 4: Create `SagaCommandHandler`**

Create `src/main/java/com/wpanther/abbreviatedtaxinvoice/pdf/application/service/SagaCommandHandler.java`:

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.application.service;

import com.wpanther.saga.domain.enums.SagaStep;
import com.wpanther.abbreviatedtaxinvoice.pdf.application.port.out.PdfStoragePort;
import com.wpanther.abbreviatedtaxinvoice.pdf.application.port.out.SagaReplyPort;
import com.wpanther.abbreviatedtaxinvoice.pdf.application.port.out.SignedXmlFetchPort;
import com.wpanther.abbreviatedtaxinvoice.pdf.application.usecase.CompensateAbbreviatedTaxInvoicePdfUseCase;
import com.wpanther.abbreviatedtaxinvoice.pdf.application.usecase.ProcessAbbreviatedTaxInvoicePdfUseCase;
import com.wpanther.abbreviatedtaxinvoice.pdf.domain.model.AbbreviatedTaxInvoicePdfDocument;
import com.wpanther.abbreviatedtaxinvoice.pdf.domain.service.AbbreviatedTaxInvoicePdfGenerationService;
import com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.in.kafka.KafkaAbbreviatedTaxInvoiceCompensateCommand;
import com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.in.kafka.KafkaAbbreviatedTaxInvoiceProcessCommand;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestClientException;

import java.util.Optional;

@Service
@Slf4j
public class SagaCommandHandler
        implements ProcessAbbreviatedTaxInvoicePdfUseCase, CompensateAbbreviatedTaxInvoicePdfUseCase {

    private static final String MDC_SAGA_ID         = "sagaId";
    private static final String MDC_CORRELATION_ID  = "correlationId";
    private static final String MDC_DOCUMENT_NUMBER = "documentNumber";
    private static final String MDC_DOCUMENT_ID     = "documentId";

    private final AbbreviatedTaxInvoicePdfDocumentService pdfDocumentService;
    private final AbbreviatedTaxInvoicePdfGenerationService pdfGenerationService;
    private final PdfStoragePort pdfStoragePort;
    private final SagaReplyPort sagaReplyPort;
    private final SignedXmlFetchPort signedXmlFetchPort;
    private final int maxRetries;

    public SagaCommandHandler(AbbreviatedTaxInvoicePdfDocumentService pdfDocumentService,
                              AbbreviatedTaxInvoicePdfGenerationService pdfGenerationService,
                              PdfStoragePort pdfStoragePort,
                              SagaReplyPort sagaReplyPort,
                              SignedXmlFetchPort signedXmlFetchPort,
                              @Value("${app.pdf.generation.max-retries:3}") int maxRetries) {
        this.pdfDocumentService = pdfDocumentService;
        this.pdfGenerationService = pdfGenerationService;
        this.pdfStoragePort = pdfStoragePort;
        this.sagaReplyPort = sagaReplyPort;
        this.signedXmlFetchPort = signedXmlFetchPort;
        this.maxRetries = maxRetries;
    }

    @Override
    public void handle(KafkaAbbreviatedTaxInvoiceProcessCommand command) {
        MDC.put(MDC_SAGA_ID,         command.getSagaId());
        MDC.put(MDC_CORRELATION_ID,  command.getCorrelationId());
        MDC.put(MDC_DOCUMENT_NUMBER, command.getDocumentNumber());
        MDC.put(MDC_DOCUMENT_ID,     command.getDocumentId());
        try {
            log.info("Handling ProcessCommand for saga {} document {}",
                    command.getSagaId(), command.getDocumentNumber());
            try {
                String signedXmlUrl = command.getSignedXmlUrl();
                String documentId   = command.getDocumentId();
                String documentNum  = command.getDocumentNumber();

                if (signedXmlUrl == null || signedXmlUrl.isBlank()) {
                    pdfDocumentService.publishGenerationFailure(command, "signedXmlUrl is null or blank");
                    return;
                }
                if (documentId == null || documentId.isBlank()) {
                    pdfDocumentService.publishGenerationFailure(command, "documentId is null or blank");
                    return;
                }
                if (documentNum == null || documentNum.isBlank()) {
                    pdfDocumentService.publishGenerationFailure(command, "documentNumber is null or blank");
                    return;
                }

                Optional<AbbreviatedTaxInvoicePdfDocument> existing =
                        pdfDocumentService.findByAbbreviatedTaxInvoiceId(documentId);

                if (existing.isPresent() && existing.get().isCompleted()) {
                    pdfDocumentService.publishIdempotentSuccess(existing.get(), command);
                    return;
                }

                int previousRetryCount = existing.map(AbbreviatedTaxInvoicePdfDocument::getRetryCount).orElse(-1);

                if (existing.isPresent() && existing.get().isMaxRetriesExceeded(maxRetries)) {
                    pdfDocumentService.publishRetryExhausted(command);
                    return;
                }

                // TX1: create GENERATING record
                AbbreviatedTaxInvoicePdfDocument document;
                if (existing.isPresent()) {
                    document = pdfDocumentService.replaceAndBeginGeneration(
                            existing.get().getId(), previousRetryCount, documentId, documentNum);
                } else {
                    document = pdfDocumentService.beginGeneration(documentId, documentNum);
                }

                String s3Key = null;
                try {
                    // NO TRANSACTION: download, generate, upload
                    String signedXml = signedXmlFetchPort.fetch(signedXmlUrl);
                    byte[] pdfBytes  = pdfGenerationService.generatePdf(documentNum, signedXml);
                    s3Key = pdfStoragePort.store(documentNum, pdfBytes);
                    String fileUrl   = pdfStoragePort.resolveUrl(s3Key);

                    // TX2: mark COMPLETED + write outbox
                    pdfDocumentService.completeGenerationAndPublish(
                            document.getId(), s3Key, fileUrl, pdfBytes.length, previousRetryCount, command);

                } catch (CallNotPermittedException e) {
                    log.warn("Circuit breaker OPEN for saga {} document {}: {}",
                            command.getSagaId(), documentNum, e.getMessage());
                    pdfDocumentService.failGenerationAndPublish(
                            document.getId(), "Circuit breaker open: " + e.getMessage(),
                            previousRetryCount, command);

                } catch (RestClientException e) {
                    log.warn("HTTP error fetching signed XML for saga {} document {}: {}",
                            command.getSagaId(), documentNum, e.getMessage());
                    pdfDocumentService.failGenerationAndPublish(
                            document.getId(), "HTTP error fetching signed XML: " + describeThrowable(e),
                            previousRetryCount, command);

                } catch (Exception e) {
                    if (s3Key != null) {
                        try { pdfStoragePort.delete(s3Key); }
                        catch (Exception del) {
                            log.error("[ORPHAN_PDF] s3Key={} saga={} error={}", s3Key,
                                    command.getSagaId(), describeThrowable(del));
                        }
                    }
                    log.error("PDF generation failed for saga {} document {}: {}",
                            command.getSagaId(), documentNum, e.getMessage(), e);
                    pdfDocumentService.failGenerationAndPublish(
                            document.getId(), describeThrowable(e), previousRetryCount, command);
                }

            } catch (OptimisticLockingFailureException e) {
                log.warn("Concurrent modification for saga {}: {}", command.getSagaId(), e.getMessage());
                pdfDocumentService.publishGenerationFailure(
                        command, "Concurrent modification: " + e.getMessage());
            } catch (Exception e) {
                log.error("Unexpected error for saga {}: {}", command.getSagaId(), e.getMessage(), e);
                pdfDocumentService.publishGenerationFailure(command, describeThrowable(e));
            }
        } finally {
            MDC.clear();
        }
    }

    @Override
    public void handle(KafkaAbbreviatedTaxInvoiceCompensateCommand command) {
        MDC.put(MDC_SAGA_ID,        command.getSagaId());
        MDC.put(MDC_CORRELATION_ID,  command.getCorrelationId());
        MDC.put(MDC_DOCUMENT_ID,     command.getDocumentId());
        try {
            log.info("Handling compensation for saga {} document {}",
                    command.getSagaId(), command.getDocumentId());
            try {
                Optional<AbbreviatedTaxInvoicePdfDocument> existing =
                        pdfDocumentService.findByAbbreviatedTaxInvoiceId(command.getDocumentId());

                if (existing.isPresent()) {
                    AbbreviatedTaxInvoicePdfDocument doc = existing.get();
                    pdfDocumentService.deleteById(doc.getId());
                    if (doc.getDocumentPath() != null) {
                        try { pdfStoragePort.delete(doc.getDocumentPath()); }
                        catch (Exception e) {
                            log.warn("Failed to delete PDF from MinIO for saga {} key {}: {}",
                                    command.getSagaId(), doc.getDocumentPath(), e.getMessage());
                        }
                    }
                    log.info("Compensated AbbreviatedTaxInvoicePdfDocument {} for saga {}",
                            doc.getId(), command.getSagaId());
                } else {
                    log.info("No document for documentId {} — already compensated",
                            command.getDocumentId());
                }
                pdfDocumentService.publishCompensated(command);

            } catch (Exception e) {
                log.error("Failed to compensate for saga {}: {}", command.getSagaId(), e.getMessage(), e);
                pdfDocumentService.publishCompensationFailure(
                        command, "Compensation failed: " + describeThrowable(e));
            }
        } finally {
            MDC.clear();
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void publishOrchestrationFailure(KafkaAbbreviatedTaxInvoiceProcessCommand command,
                                             Throwable cause) {
        try {
            sagaReplyPort.publishFailure(command.getSagaId(), command.getSagaStep(),
                    command.getCorrelationId(),
                    "Message routed to DLQ after retry exhaustion: " + describeThrowable(cause));
        } catch (Exception e) {
            log.error("Cannot notify orchestrator of DLQ failure for saga {}", command.getSagaId(), e);
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void publishCompensationOrchestrationFailure(
            KafkaAbbreviatedTaxInvoiceCompensateCommand command, Throwable cause) {
        try {
            sagaReplyPort.publishFailure(command.getSagaId(), command.getSagaStep(),
                    command.getCorrelationId(),
                    "Compensation DLQ after retry exhaustion: " + describeThrowable(cause));
        } catch (Exception e) {
            log.error("Cannot notify orchestrator of compensation DLQ failure for saga {}",
                    command.getSagaId(), e);
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void publishOrchestrationFailureForUnparsedMessage(
            String sagaId, SagaStep sagaStep, String correlationId, Throwable cause) {
        try {
            String error = "Message routed to DLQ after deserialization failure: "
                    + describeThrowable(cause);
            sagaReplyPort.publishFailure(sagaId, sagaStep, correlationId, error);
            log.error("Published FAILURE reply after DLQ routing for saga {}", sagaId);
        } catch (Exception e) {
            log.error("Cannot notify orchestrator of DLQ deserialization failure for saga {} " +
                    "— orchestrator must timeout", sagaId, e);
        }
    }

    private String describeThrowable(Throwable t) {
        if (t == null) return "unknown error";
        String msg = t.getMessage();
        return t.getClass().getSimpleName() + (msg != null ? ": " + msg : "");
    }
}
```

- [ ] **Step 5: Write `SagaCommandHandlerTest`**

Create `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/application/service/SagaCommandHandlerTest.java`:

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.application.service;

import com.wpanther.saga.domain.enums.SagaStep;
import com.wpanther.abbreviatedtaxinvoice.pdf.application.port.out.PdfStoragePort;
import com.wpanther.abbreviatedtaxinvoice.pdf.application.port.out.SagaReplyPort;
import com.wpanther.abbreviatedtaxinvoice.pdf.application.port.out.SignedXmlFetchPort;
import com.wpanther.abbreviatedtaxinvoice.pdf.domain.model.AbbreviatedTaxInvoicePdfDocument;
import com.wpanther.abbreviatedtaxinvoice.pdf.domain.model.GenerationStatus;
import com.wpanther.abbreviatedtaxinvoice.pdf.domain.service.AbbreviatedTaxInvoicePdfGenerationService;
import com.wpanther.abbreviatedtaxinvoice.pdf.domain.service.AbbreviatedTaxInvoicePdfGenerationService.AbbreviatedTaxInvoicePdfGenerationException;
import com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.in.kafka.KafkaAbbreviatedTaxInvoiceCompensateCommand;
import com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.in.kafka.KafkaAbbreviatedTaxInvoiceProcessCommand;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("SagaCommandHandler Unit Tests")
class SagaCommandHandlerTest {

    @Mock
    private AbbreviatedTaxInvoicePdfDocumentService pdfDocumentService;

    @Mock
    private AbbreviatedTaxInvoicePdfGenerationService pdfGenerationService;

    @Mock
    private PdfStoragePort pdfStoragePort;

    @Mock
    private SagaReplyPort sagaReplyPort;

    @Mock
    private SignedXmlFetchPort signedXmlFetchPort;

    private SagaCommandHandler getHandler() {
        try {
            return SagaCommandHandler.class
                    .getDeclaredConstructor(
                            AbbreviatedTaxInvoicePdfDocumentService.class,
                            AbbreviatedTaxInvoicePdfGenerationService.class,
                            PdfStoragePort.class,
                            SagaReplyPort.class,
                            SignedXmlFetchPort.class,
                            int.class)
                    .newInstance(pdfDocumentService, pdfGenerationService, pdfStoragePort,
                                 sagaReplyPort, signedXmlFetchPort, 3);
        } catch (Exception e) {
            throw new RuntimeException("Failed to instantiate SagaCommandHandler", e);
        }
    }

    private static final String SIGNED_XML_URL = "http://minio:9000/signed/abbreviated-invoice-signed.xml";
    private static final String SIGNED_XML_CONTENT = "<AbbreviatedTaxInvoice>signed</AbbreviatedTaxInvoice>";

    private KafkaAbbreviatedTaxInvoiceProcessCommand createProcessCommand() {
        return new KafkaAbbreviatedTaxInvoiceProcessCommand(
                "saga-001", SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF, "corr-456",
                "doc-123", "ATINV-2024-001", SIGNED_XML_URL);
    }

    private KafkaAbbreviatedTaxInvoiceCompensateCommand createCompensateCommand() {
        return new KafkaAbbreviatedTaxInvoiceCompensateCommand(
                "saga-001", SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF, "corr-456",
                "doc-123");
    }

    private AbbreviatedTaxInvoicePdfDocument createCompletedDocument() {
        AbbreviatedTaxInvoicePdfDocument doc = AbbreviatedTaxInvoicePdfDocument.builder()
                .id(UUID.randomUUID())
                .abbreviatedTaxInvoiceId("doc-123")
                .abbreviatedTaxInvoiceNumber("ATINV-2024-001")
                .status(GenerationStatus.COMPLETED)
                .documentPath("2024/01/15/abbreviated-tax-invoice-ATINV-2024-001-abc.pdf")
                .documentUrl("http://localhost:9000/abbreviatedtaxinvoices/2024/01/15/abbreviated-tax-invoice-ATINV-2024-001-abc.pdf")
                .fileSize(12345L)
                .build();
        return doc;
    }

    @Test
    @DisplayName("handle() process command: generates PDF and publishes SUCCESS")
    void testHandleProcessCommand_Success() throws Exception {
        KafkaAbbreviatedTaxInvoiceProcessCommand command = createProcessCommand();
        when(pdfDocumentService.findByAbbreviatedTaxInvoiceId("doc-123"))
                .thenReturn(Optional.empty());
        when(signedXmlFetchPort.fetch(SIGNED_XML_URL)).thenReturn(SIGNED_XML_CONTENT);

        AbbreviatedTaxInvoicePdfDocument generatingDoc = AbbreviatedTaxInvoicePdfDocument.builder()
                .id(UUID.randomUUID())
                .abbreviatedTaxInvoiceId("doc-123")
                .abbreviatedTaxInvoiceNumber("ATINV-2024-001")
                .status(GenerationStatus.GENERATING)
                .build();
        when(pdfDocumentService.beginGeneration("doc-123", "ATINV-2024-001"))
                .thenReturn(generatingDoc);

        byte[] pdfBytes = new byte[5000];
        when(pdfGenerationService.generatePdf(anyString(), anyString())).thenReturn(pdfBytes);
        when(pdfStoragePort.store(anyString(), any(byte[].class)))
                .thenReturn("2024/01/15/abbreviated-tax-invoice-ATINV-2024-001-abc.pdf");
        when(pdfStoragePort.resolveUrl(anyString()))
                .thenReturn("http://localhost:9000/abbreviatedtaxinvoices/2024/01/15/abbreviated-tax-invoice-ATINV-2024-001-abc.pdf");

        getHandler().handle(command);

        verify(pdfDocumentService).beginGeneration("doc-123", "ATINV-2024-001");
        verify(pdfGenerationService).generatePdf("ATINV-2024-001", SIGNED_XML_CONTENT);
        verify(pdfStoragePort).store("ATINV-2024-001", pdfBytes);
        verify(pdfDocumentService).completeGenerationAndPublish(
                eq(generatingDoc.getId()),
                eq("2024/01/15/abbreviated-tax-invoice-ATINV-2024-001-abc.pdf"),
                eq("http://localhost:9000/abbreviatedtaxinvoices/2024/01/15/abbreviated-tax-invoice-ATINV-2024-001-abc.pdf"),
                eq(5000L), eq(-1), eq(command));
    }

    @Test
    @DisplayName("handle() process command: idempotent SUCCESS for already completed document")
    void testHandleProcessCommand_AlreadyCompleted() {
        KafkaAbbreviatedTaxInvoiceProcessCommand command = createProcessCommand();
        AbbreviatedTaxInvoicePdfDocument completedDoc = createCompletedDocument();
        when(pdfDocumentService.findByAbbreviatedTaxInvoiceId("doc-123"))
                .thenReturn(Optional.of(completedDoc));

        getHandler().handle(command);

        verify(pdfDocumentService, never()).beginGeneration(anyString(), anyString());
        verify(pdfDocumentService).publishIdempotentSuccess(completedDoc, command);
    }

    @Test
    @DisplayName("handle() process command: FAILURE when max retries exceeded")
    void testHandleProcessCommand_MaxRetriesExceeded() {
        KafkaAbbreviatedTaxInvoiceProcessCommand command = createProcessCommand();
        AbbreviatedTaxInvoicePdfDocument failedDoc = AbbreviatedTaxInvoicePdfDocument.builder()
                .id(UUID.randomUUID())
                .abbreviatedTaxInvoiceId("doc-123")
                .abbreviatedTaxInvoiceNumber("ATINV-2024-001")
                .status(GenerationStatus.FAILED)
                .retryCount(3)
                .build();
        when(pdfDocumentService.findByAbbreviatedTaxInvoiceId("doc-123"))
                .thenReturn(Optional.of(failedDoc));

        getHandler().handle(command);

        verify(pdfDocumentService).publishRetryExhausted(command);
    }

    @Test
    @DisplayName("handle() process command: FAILURE on null signedXmlUrl")
    void testHandleProcessCommand_NullSignedXmlUrl() {
        KafkaAbbreviatedTaxInvoiceProcessCommand command = new KafkaAbbreviatedTaxInvoiceProcessCommand(
                "saga-001", SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF, "corr-456",
                "doc-123", "ATINV-2024-001", null);

        getHandler().handle(command);

        verify(pdfDocumentService).publishGenerationFailure(command, "signedXmlUrl is null or blank");
    }

    @Test
    @DisplayName("handle() process command: FAILURE on PDF generation failure")
    void testHandleProcessCommand_GenerationFails() throws Exception {
        KafkaAbbreviatedTaxInvoiceProcessCommand command = createProcessCommand();
        when(pdfDocumentService.findByAbbreviatedTaxInvoiceId("doc-123"))
                .thenReturn(Optional.empty());
        when(signedXmlFetchPort.fetch(SIGNED_XML_URL)).thenReturn(SIGNED_XML_CONTENT);

        AbbreviatedTaxInvoicePdfDocument generatingDoc = AbbreviatedTaxInvoicePdfDocument.builder()
                .id(UUID.randomUUID())
                .abbreviatedTaxInvoiceId("doc-123")
                .abbreviatedTaxInvoiceNumber("ATINV-2024-001")
                .status(GenerationStatus.GENERATING)
                .build();
        when(pdfDocumentService.beginGeneration("doc-123", "ATINV-2024-001"))
                .thenReturn(generatingDoc);
        when(pdfGenerationService.generatePdf(anyString(), anyString()))
                .thenThrow(new AbbreviatedTaxInvoicePdfGenerationException("FOP failed"));

        getHandler().handle(command);

        verify(pdfDocumentService).failGenerationAndPublish(
                eq(generatingDoc.getId()),
                eq("AbbreviatedTaxInvoicePdfGenerationException: FOP failed"),
                eq(-1), eq(command));
    }

    @Test
    @DisplayName("handle() compensate command: deletes document and publishes COMPENSATED")
    void testHandleCompensation_Success() {
        KafkaAbbreviatedTaxInvoiceCompensateCommand command = createCompensateCommand();
        AbbreviatedTaxInvoicePdfDocument doc = createCompletedDocument();
        when(pdfDocumentService.findByAbbreviatedTaxInvoiceId("doc-123"))
                .thenReturn(Optional.of(doc));

        getHandler().handle(command);

        verify(pdfStoragePort).delete("2024/01/15/abbreviated-tax-invoice-ATINV-2024-001-abc.pdf");
        verify(pdfDocumentService).deleteById(doc.getId());
        verify(pdfDocumentService).publishCompensated(command);
    }

    @Test
    @DisplayName("handle() compensate command: COMPENSATED even when document not found")
    void testHandleCompensation_NoDocumentFound() {
        KafkaAbbreviatedTaxInvoiceCompensateCommand command = createCompensateCommand();
        when(pdfDocumentService.findByAbbreviatedTaxInvoiceId("doc-123"))
                .thenReturn(Optional.empty());

        getHandler().handle(command);

        verify(pdfStoragePort, never()).delete(anyString());
        verify(pdfDocumentService, never()).deleteById(any());
        verify(pdfDocumentService).publishCompensated(command);
    }

    @Test
    @DisplayName("handle() compensate command: storage failure swallowed, COMPENSATED still published")
    void testHandleCompensation_StorageFailure() {
        KafkaAbbreviatedTaxInvoiceCompensateCommand command = createCompensateCommand();
        AbbreviatedTaxInvoicePdfDocument doc = createCompletedDocument();
        when(pdfDocumentService.findByAbbreviatedTaxInvoiceId("doc-123"))
                .thenReturn(Optional.of(doc));
        doThrow(new RuntimeException("MinIO unavailable")).when(pdfStoragePort).delete(anyString());

        getHandler().handle(command);

        verify(pdfDocumentService).deleteById(doc.getId());
        verify(pdfDocumentService).publishCompensated(command);
    }

    @Test
    @DisplayName("publishOrchestrationFailure() publishes failure for DLQ events")
    void testPublishOrchestrationFailure() {
        KafkaAbbreviatedTaxInvoiceProcessCommand command = createProcessCommand();

        getHandler().publishOrchestrationFailure(command, new RuntimeException("DLQ error"));

        verify(sagaReplyPort).publishFailure(
                eq("saga-001"), eq(SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF),
                eq("corr-456"), contains("Message routed to DLQ"));
    }

    @Test
    @DisplayName("publishCompensationOrchestrationFailure() publishes failure for compensation DLQ")
    void testPublishCompensationOrchestrationFailure() {
        KafkaAbbreviatedTaxInvoiceCompensateCommand command = createCompensateCommand();

        getHandler().publishCompensationOrchestrationFailure(command, new RuntimeException("Comp DLQ"));

        verify(sagaReplyPort).publishFailure(
                eq("saga-001"), eq(SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF),
                eq("corr-456"), contains("Compensation DLQ"));
    }
}
```

- [ ] **Step 6: Write `AbbreviatedTaxInvoicePdfDocumentServiceTest`**

Create `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/application/service/AbbreviatedTaxInvoicePdfDocumentServiceTest.java`:

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.application.service;

import com.wpanther.saga.domain.enums.SagaStep;
import com.wpanther.abbreviatedtaxinvoice.pdf.application.port.out.PdfEventPort;
import com.wpanther.abbreviatedtaxinvoice.pdf.application.port.out.SagaReplyPort;
import com.wpanther.abbreviatedtaxinvoice.pdf.domain.model.AbbreviatedTaxInvoicePdfDocument;
import com.wpanther.abbreviatedtaxinvoice.pdf.domain.model.GenerationStatus;
import com.wpanther.abbreviatedtaxinvoice.pdf.domain.repository.AbbreviatedTaxInvoicePdfDocumentRepository;
import com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.in.kafka.KafkaAbbreviatedTaxInvoiceCompensateCommand;
import com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.in.kafka.KafkaAbbreviatedTaxInvoiceProcessCommand;
import com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.metrics.PdfGenerationMetrics;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("AbbreviatedTaxInvoicePdfDocumentService Unit Tests")
class AbbreviatedTaxInvoicePdfDocumentServiceTest {

    @Mock
    private AbbreviatedTaxInvoicePdfDocumentRepository repository;

    @Mock
    private PdfEventPort pdfEventPort;

    @Mock
    private SagaReplyPort sagaReplyPort;

    @Mock
    private PdfGenerationMetrics pdfGenerationMetrics;

    private AbbreviatedTaxInvoicePdfDocumentService getService() {
        try {
            return AbbreviatedTaxInvoicePdfDocumentService.class
                    .getDeclaredConstructor(AbbreviatedTaxInvoicePdfDocumentRepository.class,
                                           PdfEventPort.class, SagaReplyPort.class,
                                           PdfGenerationMetrics.class)
                    .newInstance(repository, pdfEventPort, sagaReplyPort, pdfGenerationMetrics);
        } catch (Exception e) {
            throw new RuntimeException("Failed to instantiate service", e);
        }
    }

    private AbbreviatedTaxInvoicePdfDocument createGeneratingDocument() {
        AbbreviatedTaxInvoicePdfDocument doc = AbbreviatedTaxInvoicePdfDocument.builder()
                .id(UUID.randomUUID())
                .abbreviatedTaxInvoiceId("atax-inv-001")
                .abbreviatedTaxInvoiceNumber("ATINV-001")
                .status(GenerationStatus.GENERATING)
                .mimeType("application/pdf")
                .build();
        return doc;
    }

    @Test
    @DisplayName("findByAbbreviatedTaxInvoiceId() delegates to repository")
    void testFindByAbbreviatedTaxInvoiceId() {
        AbbreviatedTaxInvoicePdfDocument doc = createGeneratingDocument();
        when(repository.findByAbbreviatedTaxInvoiceId("atax-inv-001")).thenReturn(Optional.of(doc));

        Optional<AbbreviatedTaxInvoicePdfDocument> result =
                getService().findByAbbreviatedTaxInvoiceId("atax-inv-001");

        assertThat(result).isPresent();
        assertThat(result.get().getAbbreviatedTaxInvoiceNumber()).isEqualTo("ATINV-001");
    }

    @Test
    @DisplayName("beginGeneration() creates GENERATING document")
    void testBeginGeneration() {
        AbbreviatedTaxInvoicePdfDocument saved = createGeneratingDocument();
        when(repository.save(any())).thenReturn(saved);

        AbbreviatedTaxInvoicePdfDocument result =
                getService().beginGeneration("atax-inv-001", "ATINV-001");

        assertThat(result.getStatus()).isEqualTo(GenerationStatus.GENERATING);
        verify(repository).save(any());
    }

    @Test
    @DisplayName("publishCompensated() calls sagaReplyPort.publishCompensated()")
    void testPublishCompensated() {
        KafkaAbbreviatedTaxInvoiceCompensateCommand command =
                new KafkaAbbreviatedTaxInvoiceCompensateCommand(
                        "saga-001", SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF,
                        "corr-001", "doc-001");

        getService().publishCompensated(command);

        verify(sagaReplyPort).publishCompensated("saga-001",
                SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF, "corr-001");
    }

    @Test
    @DisplayName("publishGenerationFailure() calls sagaReplyPort.publishFailure()")
    void testPublishGenerationFailure() {
        KafkaAbbreviatedTaxInvoiceProcessCommand command =
                new KafkaAbbreviatedTaxInvoiceProcessCommand(
                        "saga-001", SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF,
                        "corr-001", "doc-001", "ATINV-001", "http://url");

        getService().publishGenerationFailure(command, "error msg");

        verify(sagaReplyPort).publishFailure("saga-001",
                SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF, "corr-001", "error msg");
    }

    @Test
    @DisplayName("publishRetryExhausted() records metric and calls publishFailure()")
    void testPublishRetryExhausted() {
        KafkaAbbreviatedTaxInvoiceProcessCommand command =
                new KafkaAbbreviatedTaxInvoiceProcessCommand(
                        "saga-001", SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF,
                        "corr-001", "doc-001", "ATINV-001", "http://url");

        getService().publishRetryExhausted(command);

        verify(pdfGenerationMetrics).recordRetryExhausted("saga-001", "doc-001", "ATINV-001");
        verify(sagaReplyPort).publishFailure(eq("saga-001"),
                eq(SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF), eq("corr-001"),
                eq("Maximum retry attempts exceeded"));
    }
}
```

- [ ] **Step 7: Run all tests written so far**

```bash
cd /home/wpanther/projects/etax/invoice-microservices/services/abbreviatedtaxinvoice-pdf-generation-service
mvn test -Dtest="RestTemplateSignedXmlFetcherTest,SagaCommandHandlerTest,AbbreviatedTaxInvoicePdfDocumentServiceTest" 2>&1 | tail -15
```

Expected: `BUILD SUCCESS`, all tests pass.

- [ ] **Step 8: Run full test suite to check coverage**

```bash
mvn verify 2>&1 | tail -20
```

Expected: `BUILD SUCCESS`. If JaCoCo coverage fails, add missing tests to reach 90% line coverage.

- [ ] **Step 9: Commit**

```bash
git add src/
git commit -m "feat: add REST client, application services (DocumentService + SagaCommandHandler)"
```

---

---

## Task 15: `application.yml` + `application-test.yml` + CamelRouteConfigTest + KafkaCommandMapperTest

**Files:**
- Create: `src/main/resources/application.yml`
- Create: `src/test/resources/application-test.yml`
- Test: `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/config/CamelRouteConfigTest.java`
- Test: `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/in/kafka/KafkaCommandMapperTest.java`

- [ ] **Step 1: Create `application.yml`**

Create `src/main/resources/application.yml`:

```yaml
server:
  port: 8096

spring:
  application:
    name: abbreviatedtaxinvoice-pdf-generation-service

  datasource:
    url: jdbc:postgresql://${DB_HOST:localhost}:${DB_PORT:5432}/${DB_NAME:abbreviatedtaxinvoicepdf_db}
    username: ${DB_USERNAME:postgres}
    password: ${DB_PASSWORD:postgres}
    driver-class-name: org.postgresql.Driver
    hikari:
      maximum-pool-size: 10
      minimum-idle: 5

  jpa:
    database-platform: org.hibernate.dialect.PostgreSQLDialect
    hibernate:
      ddl-auto: validate
    show-sql: false

  flyway:
    enabled: true
    locations: classpath:db/migration

  jackson:
    serialization:
      write-dates-as-timestamps: false

# Apache Camel
camel:
  springboot:
    name: abbreviatedtaxinvoice-pdf-generation-camel
    main-run-controller: true
  dataformat:
    jackson:
      auto-discover-object-mapper: true

# Application-specific configuration
app:
  kafka:
    bootstrap-servers: ${KAFKA_BROKERS:localhost:9092}
    consumer:
      command-group-id: ${KAFKA_COMMAND_GROUP_ID:abbreviated-tax-invoice-pdf-generation-command}
      compensation-group-id: ${KAFKA_COMPENSATION_GROUP_ID:abbreviated-tax-invoice-pdf-generation-compensation}
      break-on-first-error: ${KAFKA_BREAK_ON_FIRST_ERROR:true}
      max-poll-records: ${KAFKA_MAX_POLL_RECORDS:100}
      consumers-count: ${KAFKA_CONSUMERS_COUNT:3}
    topics:
      saga-command-abbreviated-tax-invoice-pdf: saga.command.abbreviated-tax-invoice-pdf
      saga-compensation-abbreviated-tax-invoice-pdf: saga.compensation.abbreviated-tax-invoice-pdf
      pdf-generated-abbreviated-tax-invoice: pdf.generated.abbreviated-tax-invoice
      dlq: pdf.generation.abbreviated-tax-invoice.dlq
  minio:
    endpoint: ${MINIO_ENDPOINT:http://localhost:9000}
    access-key: ${MINIO_ACCESS_KEY:minioadmin}
    secret-key: ${MINIO_SECRET_KEY:minioadmin}
    bucket-name: ${MINIO_BUCKET_NAME:abbreviatedtaxinvoices}
    region: ${MINIO_REGION:us-east-1}
    base-url: ${MINIO_BASE_URL:http://localhost:9000/abbreviatedtaxinvoices}
    path-style-access: ${MINIO_PATH_STYLE_ACCESS:true}
    cleanup:
      enabled: ${MINIO_CLEANUP_ENABLED:false}
      cron: ${MINIO_CLEANUP_CRON:0 0 2 * * ?}
  pdf:
    icc-profile-path: ${PDF_ICC_PROFILE_PATH:icc/sRGB.icc}
    generation:
      max-retries: ${PDF_GENERATION_MAX_RETRIES:3}
      max-concurrent-renders: ${PDF_MAX_CONCURRENT_RENDERS:3}
      max-pdf-size-bytes: ${PDF_MAX_SIZE_BYTES:52428800}
  rest-client:
    connect-timeout: ${REST_CLIENT_CONNECT_TIMEOUT:5000}
    read-timeout: ${REST_CLIENT_READ_TIMEOUT:10000}
    allowed-hosts: ${REST_CLIENT_ALLOWED_HOSTS:localhost}
  fonts:
    health-check:
      enabled: ${FONT_HEALTH_CHECK_ENABLED:true}
      fail-on-error: ${FONT_HEALTH_CHECK_FAIL_ON_ERROR:true}

# Resilience4j Circuit Breaker
resilience4j:
  circuitbreaker:
    instances:
      minio:
        sliding-window-size: 10
        failure-rate-threshold: 50
        wait-duration-in-open-state: 30s
        permitted-number-of-calls-in-half-open-state: 3
        automatic-transition-from-open-to-half-open-enabled: true
        register-health-indicator: true
      signedXmlFetch:
        sliding-window-size: 10
        failure-rate-threshold: 50
        wait-duration-in-open-state: 60s
        permitted-number-of-calls-in-half-open-state: 3
        slow-call-rate-threshold: 50
        slow-call-duration-threshold: 3s

# Eureka client configuration
eureka:
  client:
    service-url:
      defaultZone: ${EUREKA_URL:http://localhost:8761/eureka/}
    register-with-eureka: true
    fetch-registry: true
  instance:
    prefer-ip-address: true
    instance-id: ${spring.application.name}:${random.value}

# Actuator endpoints
management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics,prometheus,camelroutes
  endpoint:
    health:
      show-details: when-authorized
  metrics:
    tags:
      application: ${spring.application.name}
  tracing:
    sampling:
      probability: ${TRACING_SAMPLING_PROBABILITY:1.0}
  otlp:
    tracing:
      endpoint: ${OTEL_EXPORTER_OTLP_ENDPOINT:http://localhost:4318/v1/traces}

# Logging
logging:
  level:
    root: INFO
    com.wpanther.abbreviatedtaxinvoice.pdf: INFO
    org.apache.camel: INFO
    org.apache.camel.component.kafka: DEBUG
    org.apache.fop: INFO
    org.apache.pdfbox: INFO
```

- [ ] **Step 2: Create `application-test.yml`**

Create `src/test/resources/application-test.yml`:

```yaml
spring:
  application:
    name: abbreviatedtaxinvoice-pdf-generation-service-test

  datasource:
    url: jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE
    username: sa
    password:
    driver-class-name: org.h2.Driver

  jpa:
    database-platform: org.hibernate.dialect.H2Dialect
    hibernate:
      ddl-auto: create-drop
    show-sql: false

  flyway:
    enabled: false

# Apache Camel - disabled for unit tests
camel:
  springboot:
    name: abbreviatedtaxinvoice-pdf-generation-camel-test
    main-run-controller: false
  dataformat:
    jackson:
      auto-discover-object-mapper: true

# Application configuration for tests
app:
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: abbreviatedtaxinvoice-pdf-generation-service-test
    topics:
      saga-command-abbreviated-tax-invoice-pdf: saga.command.abbreviated-tax-invoice-pdf
      saga-compensation-abbreviated-tax-invoice-pdf: saga.compensation.abbreviated-tax-invoice-pdf
      pdf-generated-abbreviated-tax-invoice: pdf.generated.abbreviated-tax-invoice
      dlq: pdf.generation.abbreviated-tax-invoice.dlq
  minio:
    endpoint: http://localhost:9000
    access-key: minioadmin
    secret-key: minioadmin
    bucket-name: abbreviatedtaxinvoices-test
    region: us-east-1
    base-url: http://localhost:9000/abbreviatedtaxinvoices-test
    path-style-access: true

# Disable Eureka for tests
eureka:
  client:
    enabled: false

logging:
  level:
    root: WARN
    com.wpanther.abbreviatedtaxinvoice.pdf: DEBUG
    org.apache.camel: WARN
```

- [ ] **Step 3: Write `CamelRouteConfigTest`**

Create `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/config/CamelRouteConfigTest.java`:

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.wpanther.saga.domain.enums.SagaStep;
import com.wpanther.abbreviatedtaxinvoice.pdf.application.service.SagaCommandHandler;
import com.wpanther.abbreviatedtaxinvoice.pdf.application.usecase.CompensateAbbreviatedTaxInvoicePdfUseCase;
import com.wpanther.abbreviatedtaxinvoice.pdf.application.usecase.ProcessAbbreviatedTaxInvoicePdfUseCase;
import com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.in.kafka.KafkaAbbreviatedTaxInvoiceCompensateCommand;
import com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.in.kafka.KafkaAbbreviatedTaxInvoiceProcessCommand;
import com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.in.kafka.SagaRouteConfig;
import com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.messaging.AbbreviatedTaxInvoicePdfGeneratedEvent;
import com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.messaging.AbbreviatedTaxInvoicePdfReplyEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
@DisplayName("CamelRouteConfig Unit Tests")
class CamelRouteConfigTest {

    @Mock
    private ProcessAbbreviatedTaxInvoicePdfUseCase processUseCase;

    @Mock
    private CompensateAbbreviatedTaxInvoicePdfUseCase compensateUseCase;

    @Mock
    private SagaCommandHandler sagaCommandHandler;

    private ObjectMapper objectMapper;
    private SagaRouteConfig sagaRouteConfig;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        sagaRouteConfig = new SagaRouteConfig(processUseCase, compensateUseCase,
                sagaCommandHandler, objectMapper);
    }

    @Test
    @DisplayName("Should serialize and deserialize KafkaAbbreviatedTaxInvoiceProcessCommand")
    void testProcessCommandSerialization() throws Exception {
        KafkaAbbreviatedTaxInvoiceProcessCommand command = new KafkaAbbreviatedTaxInvoiceProcessCommand(
                "saga-001", SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF, "corr-456",
                "doc-123", "ATINV-2024-001",
                "http://minio/abbreviated-invoice-signed.xml"
        );

        String json = objectMapper.writeValueAsString(command);
        KafkaAbbreviatedTaxInvoiceProcessCommand deserialized =
                objectMapper.readValue(json, KafkaAbbreviatedTaxInvoiceProcessCommand.class);

        assertThat(deserialized.getSagaId()).isEqualTo("saga-001");
        assertThat(deserialized.getSagaStep()).isEqualTo(SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF);
        assertThat(deserialized.getCorrelationId()).isEqualTo("corr-456");
        assertThat(deserialized.getDocumentId()).isEqualTo("doc-123");
        assertThat(deserialized.getDocumentNumber()).isEqualTo("ATINV-2024-001");
        assertThat(deserialized.getSignedXmlUrl()).isEqualTo("http://minio/abbreviated-invoice-signed.xml");
        assertThat(deserialized.getEventId()).isNotNull();
    }

    @Test
    @DisplayName("Should serialize and deserialize KafkaAbbreviatedTaxInvoiceCompensateCommand")
    void testCompensateCommandSerialization() throws Exception {
        KafkaAbbreviatedTaxInvoiceCompensateCommand command =
                new KafkaAbbreviatedTaxInvoiceCompensateCommand(
                        "saga-001", SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF,
                        "corr-456", "doc-123");

        String json = objectMapper.writeValueAsString(command);
        KafkaAbbreviatedTaxInvoiceCompensateCommand deserialized =
                objectMapper.readValue(json, KafkaAbbreviatedTaxInvoiceCompensateCommand.class);

        assertThat(deserialized.getSagaId()).isEqualTo("saga-001");
        assertThat(deserialized.getSagaStep()).isEqualTo(SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF);
        assertThat(deserialized.getCorrelationId()).isEqualTo("corr-456");
        assertThat(deserialized.getDocumentId()).isEqualTo("doc-123");
    }

    @Test
    @DisplayName("Should serialize AbbreviatedTaxInvoicePdfGeneratedEvent with correct eventType")
    void testGeneratedEventSerialization() throws Exception {
        AbbreviatedTaxInvoicePdfGeneratedEvent event = new AbbreviatedTaxInvoicePdfGeneratedEvent(
                "saga-001", "doc-123", "ATINV-2024-001",
                "http://example.com/doc.pdf", 12345L, true, "corr-456");

        String json = objectMapper.writeValueAsString(event);

        assertThat(json).contains("\"eventType\":\"pdf.generated.abbreviated-tax-invoice\"");
        assertThat(json).contains("\"eventId\"");
        assertThat(event.getEventId()).isNotNull();
        assertThat(event.getOccurredAt()).isNotNull();
        assertThat(event.getVersion()).isEqualTo(1);
    }

    @Test
    @DisplayName("Should create AbbreviatedTaxInvoicePdfReplyEvent with correct statuses")
    void testReplyEventCreation() throws Exception {
        AbbreviatedTaxInvoicePdfReplyEvent successReply = AbbreviatedTaxInvoicePdfReplyEvent.success(
                "saga-001", SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF, "corr-456",
                "http://localhost:9000/abbreviatedtaxinvoices/test.pdf", 12345L);
        AbbreviatedTaxInvoicePdfReplyEvent failureReply = AbbreviatedTaxInvoicePdfReplyEvent.failure(
                "saga-001", SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF, "corr-456", "error msg");
        AbbreviatedTaxInvoicePdfReplyEvent compensatedReply = AbbreviatedTaxInvoicePdfReplyEvent.compensated(
                "saga-001", SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF, "corr-456");

        assertThat(successReply.isSuccess()).isTrue();
        assertThat(successReply.getSagaId()).isEqualTo("saga-001");

        assertThat(failureReply.isFailure()).isTrue();
        assertThat(failureReply.getErrorMessage()).isEqualTo("error msg");

        assertThat(compensatedReply.isCompensated()).isTrue();

        String json = objectMapper.writeValueAsString(successReply);
        assertThat(json).contains("\"sagaId\":\"saga-001\"");
        assertThat(json).contains("\"status\":\"SUCCESS\"");
    }

    @Test
    @DisplayName("Should deserialize KafkaAbbreviatedTaxInvoiceProcessCommand from JSON")
    void testProcessCommandDeserialization() throws Exception {
        String json = """
            {
                "eventId": "550e8400-e29b-41d4-a716-446655440000",
                "occurredAt": "2024-01-15T10:30:00Z",
                "eventType": "saga.command.abbreviated-tax-invoice-pdf",
                "version": 1,
                "sagaId": "saga-001",
                "sagaStep": "generate-abbreviated-tax-invoice-pdf",
                "correlationId": "corr-456",
                "documentId": "doc-123",
                "documentNumber": "ATINV-2024-001",
                "signedXmlUrl": "http://minio/abbreviated-invoice-signed.xml"
            }
            """;

        KafkaAbbreviatedTaxInvoiceProcessCommand cmd =
                objectMapper.readValue(json, KafkaAbbreviatedTaxInvoiceProcessCommand.class);

        assertThat(cmd.getEventId()).isEqualTo(UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));
        assertThat(cmd.getSagaId()).isEqualTo("saga-001");
        assertThat(cmd.getSagaStep()).isEqualTo(SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF);
        assertThat(cmd.getCorrelationId()).isEqualTo("corr-456");
        assertThat(cmd.getDocumentId()).isEqualTo("doc-123");
        assertThat(cmd.getDocumentNumber()).isEqualTo("ATINV-2024-001");
        assertThat(cmd.getSignedXmlUrl()).isEqualTo("http://minio/abbreviated-invoice-signed.xml");
    }
}
```

- [ ] **Step 4: Write `KafkaCommandMapperTest`**

Create `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/in/kafka/KafkaCommandMapperTest.java`:

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.in.kafka;

import com.wpanther.saga.domain.enums.SagaStep;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class KafkaCommandMapperTest {

    private final KafkaCommandMapper mapper = new KafkaCommandMapper();

    @Test
    void toProcess_mapsAllFields() {
        var src = new KafkaAbbreviatedTaxInvoiceProcessCommand(
                null, null, null, 0,
                "saga-1", SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF, "corr-1",
                "doc-1", "ATINV-001", "http://minio/xml");

        var result = mapper.toProcess(src);

        assertThat(result.getSagaId()).isEqualTo("saga-1");
        assertThat(result.getCorrelationId()).isEqualTo("corr-1");
        assertThat(result.getDocumentId()).isEqualTo("doc-1");
        assertThat(result.getDocumentNumber()).isEqualTo("ATINV-001");
        assertThat(result.getSignedXmlUrl()).isEqualTo("http://minio/xml");
    }

    @Test
    void toCompensate_mapsAllFields() {
        var src = new KafkaAbbreviatedTaxInvoiceCompensateCommand(
                null, null, null, 0,
                "saga-2", SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF, "corr-2",
                "doc-2");

        var result = mapper.toCompensate(src);

        assertThat(result.getSagaId()).isEqualTo("saga-2");
        assertThat(result.getCorrelationId()).isEqualTo("corr-2");
        assertThat(result.getDocumentId()).isEqualTo("doc-2");
    }
}
```

- [ ] **Step 5: Run tests**

```bash
cd /home/wpanther/projects/etax/invoice-microservices/services/abbreviatedtaxinvoice-pdf-generation-service
mvn test -Dtest="CamelRouteConfigTest,KafkaCommandMapperTest" 2>&1 | tail -10
```

Expected: `BUILD SUCCESS`.

- [ ] **Step 6: Commit**

```bash
git add src/
git commit -m "feat: add application.yml, test config, Camel route serialization tests, command mapper tests"
```

---

## Task 16: Remaining Domain + Persistence + Messaging Tests

This task covers the test classes not yet written: domain model, exception, constants, repository adapter, event publisher, saga reply publisher, service impl, and Thai amount words converter.

**Files:**
- Test: `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/domain/model/AbbreviatedTaxInvoicePdfDocumentTest.java`
- Test: `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/domain/exception/AbbreviatedTaxInvoicePdfGenerationExceptionTest.java`
- Test: `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/domain/constants/PdfGenerationConstantsTest.java`
- Test: `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/persistence/AbbreviatedTaxInvoicePdfDocumentRepositoryAdapterTest.java`
- Test: `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/messaging/EventPublisherTest.java`
- Test: `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/messaging/SagaReplyPublisherTest.java`
- Test: `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/pdf/AbbreviatedTaxInvoicePdfGenerationServiceImplTest.java`
- Test: `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/pdf/ThaiAmountWordsConverterTest.java`

- [ ] **Step 1: Create `AbbreviatedTaxInvoicePdfDocumentTest`**

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
                .abbreviatedTaxInvoiceId("atax-inv-001")
                .abbreviatedTaxInvoiceNumber("ATINV-2024-001")
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
                        .abbreviatedTaxInvoiceNumber("ATINV-001")
                        .build()
        ).isInstanceOf(AbbreviatedTaxInvoicePdfGenerationException.class)
         .hasMessageContaining("Abbreviated Tax Invoice ID cannot be blank");
    }

    @Test
    @DisplayName("Should reject null abbreviatedTaxInvoiceNumber")
    void testCreate_NullNumber() {
        assertThatThrownBy(() ->
                AbbreviatedTaxInvoicePdfDocument.builder()
                        .abbreviatedTaxInvoiceId("atax-inv-001")
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
        doc.markCompleted("2024/01/15/test.pdf", "http://minio/test.pdf", 12345L);

        assertThat(doc.getStatus()).isEqualTo(GenerationStatus.COMPLETED);
        assertThat(doc.getDocumentPath()).isEqualTo("2024/01/15/test.pdf");
        assertThat(doc.getDocumentUrl()).isEqualTo("http://minio/test.pdf");
        assertThat(doc.getFileSize()).isEqualTo(12345L);
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
    @DisplayName("PENDING → markXmlEmbedded() sets flag")
    void testMarkXmlEmbedded() {
        AbbreviatedTaxInvoicePdfDocument doc = pendingDocument();
        assertThat(doc.isXmlEmbedded()).isFalse();
        doc.markXmlEmbedded();
        assertThat(doc.isXmlEmbedded()).isTrue();
    }

    @Test
    @DisplayName("startGeneration() from GENERATING throws")
    void testStartGeneration_AlreadyGenerating() {
        AbbreviatedTaxInvoicePdfDocument doc = pendingDocument();
        doc.startGeneration();

        assertThatThrownBy(doc::startGeneration)
                .isInstanceOf(AbbreviatedTaxInvoicePdfGenerationException.class)
                .hasMessageContaining("PENDING");
    }

    @Test
    @DisplayName("markCompleted() from PENDING throws")
    void testMarkCompleted_FromPending() {
        AbbreviatedTaxInvoicePdfDocument doc = pendingDocument();

        assertThatThrownBy(() -> doc.markCompleted("path", "url", 100L))
                .isInstanceOf(AbbreviatedTaxInvoicePdfGenerationException.class)
                .hasMessageContaining("GENERATING");
    }

    @Test
    @DisplayName("markCompleted() with zero fileSize throws")
    void testMarkCompleted_ZeroFileSize() {
        AbbreviatedTaxInvoicePdfDocument doc = pendingDocument();
        doc.startGeneration();

        assertThatThrownBy(() -> doc.markCompleted("path", "url", 0L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("File size must be positive");
    }

    @Test
    @DisplayName("markCompleted() with null documentPath throws NPE")
    void testMarkCompleted_NullPath() {
        AbbreviatedTaxInvoicePdfDocument doc = pendingDocument();
        doc.startGeneration();

        assertThatThrownBy(() -> doc.markCompleted(null, "url", 100L))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    @DisplayName("incrementRetryCount() increases count by one")
    void testIncrementRetryCount() {
        AbbreviatedTaxInvoicePdfDocument doc = pendingDocument();
        assertThat(doc.getRetryCount()).isZero();
        doc.incrementRetryCount();
        assertThat(doc.getRetryCount()).isOne();
        doc.incrementRetryCount();
        assertThat(doc.getRetryCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("incrementRetryCountTo() advances count to target")
    void testIncrementRetryCountTo_AdvancesToTarget() {
        AbbreviatedTaxInvoicePdfDocument doc = pendingDocument();
        doc.incrementRetryCountTo(2);
        assertThat(doc.getRetryCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("incrementRetryCountTo() is a no-op when count already at target")
    void testIncrementRetryCountTo_NoOpWhenAlreadyAtTarget() {
        AbbreviatedTaxInvoicePdfDocument doc = AbbreviatedTaxInvoicePdfDocument.builder()
                .abbreviatedTaxInvoiceId("atax-inv-001")
                .abbreviatedTaxInvoiceNumber("ATINV-001")
                .retryCount(2)
                .build();
        doc.incrementRetryCountTo(1);
        assertThat(doc.getRetryCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("isMaxRetriesExceeded() returns true when retryCount >= maxRetries")
    void testIsMaxRetriesExceeded_AtLimit() {
        AbbreviatedTaxInvoicePdfDocument doc = AbbreviatedTaxInvoicePdfDocument.builder()
                .abbreviatedTaxInvoiceId("atax-inv-001")
                .abbreviatedTaxInvoiceNumber("ATINV-001")
                .retryCount(3)
                .build();
        assertThat(doc.isMaxRetriesExceeded(3)).isTrue();
    }

    @Test
    @DisplayName("isMaxRetriesExceeded() returns false when retryCount < maxRetries")
    void testIsMaxRetriesExceeded_BelowLimit() {
        AbbreviatedTaxInvoicePdfDocument doc = AbbreviatedTaxInvoicePdfDocument.builder()
                .abbreviatedTaxInvoiceId("atax-inv-001")
                .abbreviatedTaxInvoiceNumber("ATINV-001")
                .retryCount(2)
                .build();
        assertThat(doc.isMaxRetriesExceeded(3)).isFalse();
    }
}
```

- [ ] **Step 2: Create `AbbreviatedTaxInvoicePdfGenerationExceptionTest`**

Create `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/domain/exception/AbbreviatedTaxInvoicePdfGenerationExceptionTest.java`:

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.domain.exception;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class AbbreviatedTaxInvoicePdfGenerationExceptionTest {

    @Test
    void constructor_withMessage_storesMessage() {
        var ex = new AbbreviatedTaxInvoicePdfGenerationException("test error");
        assertThat(ex.getMessage()).isEqualTo("test error");
    }

    @Test
    void constructor_withMessageAndCause_storesBoth() {
        var cause = new RuntimeException("root");
        var ex = new AbbreviatedTaxInvoicePdfGenerationException("wrapped", cause);
        assertThat(ex.getMessage()).isEqualTo("wrapped");
        assertThat(ex.getCause()).isSameAs(cause);
    }

    @Test
    void isRuntimeException() {
        assertThat(new AbbreviatedTaxInvoicePdfGenerationException("x"))
                .isInstanceOf(RuntimeException.class);
    }
}
```

- [ ] **Step 3: Create `PdfGenerationConstantsTest`**

Create `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/domain/constants/PdfGenerationConstantsTest.java`:

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.domain.constants;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("PdfGenerationConstants Unit Tests")
class PdfGenerationConstantsTest {

    @Test
    @DisplayName("DEFAULT_MAX_RETRIES should be 3")
    void testDefaultMaxRetries() {
        assertThat(PdfGenerationConstants.DEFAULT_MAX_RETRIES).isEqualTo(3);
    }

    @Test
    @DisplayName("PDF_MIME_TYPE should be 'application/pdf'")
    void testPdfMimeType() {
        assertThat(PdfGenerationConstants.PDF_MIME_TYPE).isEqualTo("application/pdf");
    }

    @Test
    @DisplayName("Constructor should be private - utility class pattern")
    void testConstructorIsPrivate() throws Exception {
        var constructor = PdfGenerationConstants.class.getDeclaredConstructor();
        assertThat(java.lang.reflect.Modifier.isPrivate(constructor.getModifiers())).isTrue();
    }
}
```

- [ ] **Step 4: Create `AbbreviatedTaxInvoicePdfDocumentRepositoryAdapterTest`**

Create `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/persistence/AbbreviatedTaxInvoicePdfDocumentRepositoryAdapterTest.java`:

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
class AbbreviatedTaxInvoicePdfDocumentRepositoryAdapterTest {

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
                .abbreviatedTaxInvoiceId("atax-inv-123")
                .abbreviatedTaxInvoiceNumber("ATINV-2024-001")
                .documentPath("2024/01/15/abbreviated-tax-invoice-ATINV-2024-001-abc.pdf")
                .documentUrl("http://localhost:9000/abbreviatedtaxinvoices/2024/01/15/abbreviated-tax-invoice-ATINV-2024-001-abc.pdf")
                .fileSize(12345L)
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
                .abbreviatedTaxInvoiceId("atax-inv-123")
                .abbreviatedTaxInvoiceNumber("ATINV-2024-001")
                .documentPath("2024/01/15/abbreviated-tax-invoice-ATINV-2024-001-abc.pdf")
                .documentUrl("http://localhost:9000/abbreviatedtaxinvoices/2024/01/15/abbreviated-tax-invoice-ATINV-2024-001-abc.pdf")
                .fileSize(12345L)
                .mimeType("application/pdf")
                .xmlEmbedded(true)
                .status(GenerationStatus.COMPLETED)
                .errorMessage(null)
                .retryCount(1)
                .createdAt(LocalDateTime.now())
                .completedAt(LocalDateTime.now())
                .build();
        when(jpaRepository.save(any())).thenReturn(entity);

        AbbreviatedTaxInvoicePdfDocument result = repository.save(domain);

        verify(jpaRepository).save(any(AbbreviatedTaxInvoicePdfDocumentEntity.class));
        assertThat(result.getAbbreviatedTaxInvoiceId()).isEqualTo("atax-inv-123");
        assertThat(result.getAbbreviatedTaxInvoiceNumber()).isEqualTo("ATINV-2024-001");
        assertThat(result.getFileSize()).isEqualTo(12345L);
        assertThat(result.isXmlEmbedded()).isTrue();
        assertThat(result.getStatus()).isEqualTo(GenerationStatus.COMPLETED);
    }

    @Test
    @DisplayName("findById() returns mapped domain when found")
    void testFindById_found() {
        AbbreviatedTaxInvoicePdfDocumentEntity entity = AbbreviatedTaxInvoicePdfDocumentEntity.builder()
                .id(id)
                .abbreviatedTaxInvoiceId("atax-inv-123")
                .abbreviatedTaxInvoiceNumber("ATINV-2024-001")
                .status(GenerationStatus.COMPLETED)
                .fileSize(12345L)
                .xmlEmbedded(true)
                .retryCount(0)
                .mimeType("application/pdf")
                .build();
        when(jpaRepository.findById(id)).thenReturn(Optional.of(entity));

        Optional<AbbreviatedTaxInvoicePdfDocument> result = repository.findById(id);

        assertThat(result).isPresent();
        assertThat(result.get().getAbbreviatedTaxInvoiceId()).isEqualTo("atax-inv-123");
    }

    @Test
    @DisplayName("findById() returns empty when not found")
    void testFindById_notFound() {
        when(jpaRepository.findById(id)).thenReturn(Optional.empty());
        Optional<AbbreviatedTaxInvoicePdfDocument> result = repository.findById(id);
        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("findByAbbreviatedTaxInvoiceId() returns mapped domain when found")
    void testFindByAbbreviatedTaxInvoiceId_found() {
        AbbreviatedTaxInvoicePdfDocumentEntity entity = AbbreviatedTaxInvoicePdfDocumentEntity.builder()
                .id(id)
                .abbreviatedTaxInvoiceId("atax-inv-123")
                .abbreviatedTaxInvoiceNumber("ATINV-2024-001")
                .status(GenerationStatus.COMPLETED)
                .fileSize(12345L)
                .xmlEmbedded(true)
                .retryCount(0)
                .mimeType("application/pdf")
                .build();
        when(jpaRepository.findByAbbreviatedTaxInvoiceId("atax-inv-123"))
                .thenReturn(Optional.of(entity));

        Optional<AbbreviatedTaxInvoicePdfDocument> result =
                repository.findByAbbreviatedTaxInvoiceId("atax-inv-123");

        assertThat(result).isPresent();
        assertThat(result.get().getAbbreviatedTaxInvoiceId()).isEqualTo("atax-inv-123");
    }

    @Test
    @DisplayName("findByAbbreviatedTaxInvoiceId() returns empty when not found")
    void testFindByAbbreviatedTaxInvoiceId_notFound() {
        when(jpaRepository.findByAbbreviatedTaxInvoiceId("unknown"))
                .thenReturn(Optional.empty());

        Optional<AbbreviatedTaxInvoicePdfDocument> result =
                repository.findByAbbreviatedTaxInvoiceId("unknown");

        assertThat(result).isEmpty();
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

- [ ] **Step 5: Create `EventPublisherTest`**

Create `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/messaging/EventPublisherTest.java`:

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.wpanther.saga.infrastructure.outbox.OutboxService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
@DisplayName("EventPublisher Unit Tests")
class EventPublisherTest {

    @Mock
    private OutboxService outboxService;

    private ObjectMapper objectMapper;
    private EventPublisher eventPublisher;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        eventPublisher = new EventPublisher(outboxService, objectMapper);
    }

    @Test
    @DisplayName("publishPdfGenerated() calls OutboxService with correct parameters")
    void testPublishPdfGenerated() {
        AbbreviatedTaxInvoicePdfGeneratedEvent event = new AbbreviatedTaxInvoicePdfGeneratedEvent(
                "saga-001", "doc-123", "ATINV-2024-001",
                "http://localhost:9000/abbreviatedtaxinvoices/test.pdf", 12345L, true, "corr-456");

        eventPublisher.publishPdfGenerated(event);

        verify(outboxService).saveWithRouting(
                eq(event),
                eq("AbbreviatedTaxInvoicePdfDocument"),
                eq("doc-123"),
                eq("pdf.generated.abbreviated-tax-invoice"),
                eq("doc-123"),
                anyString());
    }

    @Test
    @DisplayName("publishPdfGenerated() includes documentType header")
    void testPublishPdfGenerated_Headers() {
        AbbreviatedTaxInvoicePdfGeneratedEvent event = new AbbreviatedTaxInvoicePdfGeneratedEvent(
                "saga-001", "doc-123", "ATINV-001",
                "http://localhost:9000/abbreviatedtaxinvoices/test.pdf", 12345L, true, "corr-456");

        eventPublisher.publishPdfGenerated(event);

        ArgumentCaptor<String> headersCaptor = ArgumentCaptor.forClass(String.class);
        verify(outboxService).saveWithRouting(
                any(), anyString(), anyString(), anyString(), anyString(),
                headersCaptor.capture());

        String headersJson = headersCaptor.getValue();
        assertThat(headersJson).contains("\"documentType\":\"ABBREVIATED_TAX_INVOICE\"");
        assertThat(headersJson).contains("\"correlationId\":\"corr-456\"");
    }

    @Test
    @DisplayName("Generated event stores sagaId and correlationId independently")
    void testSagaIdAndCorrelationIdStoredIndependently() throws Exception {
        AbbreviatedTaxInvoicePdfGeneratedEvent event = new AbbreviatedTaxInvoicePdfGeneratedEvent(
                "saga-001", "doc-123", "ATINV-2024-001",
                "http://localhost:9000/abbreviatedtaxinvoices/test.pdf", 12345L, true, "corr-456");

        assertThat(event.getSagaId()).isEqualTo("saga-001");
        assertThat(event.getCorrelationId()).isEqualTo("corr-456");

        String json = objectMapper.writeValueAsString(event);
        AbbreviatedTaxInvoicePdfGeneratedEvent deserialized =
                objectMapper.readValue(json, AbbreviatedTaxInvoicePdfGeneratedEvent.class);
        assertThat(deserialized.getSagaId()).isEqualTo("saga-001");
        assertThat(deserialized.getCorrelationId()).isEqualTo("corr-456");
    }
}
```

- [ ] **Step 6: Create `SagaReplyPublisherTest`**

Create `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/messaging/SagaReplyPublisherTest.java`:

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.wpanther.saga.domain.enums.SagaStep;
import com.wpanther.saga.infrastructure.outbox.OutboxService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
@DisplayName("SagaReplyPublisher Unit Tests")
class SagaReplyPublisherTest {

    @Mock
    private OutboxService outboxService;

    private ObjectMapper objectMapper;
    private SagaReplyPublisher sagaReplyPublisher;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        sagaReplyPublisher = new SagaReplyPublisher(outboxService, objectMapper);
    }

    @Test
    @DisplayName("publishSuccess() calls OutboxService with SUCCESS reply")
    void testPublishSuccess() {
        sagaReplyPublisher.publishSuccess(
                "saga-123", SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF,
                "corr-456", "http://localhost:9000/abbreviatedtaxinvoices/test.pdf", 12345L);

        verify(outboxService).saveWithRouting(
                any(AbbreviatedTaxInvoicePdfReplyEvent.class),
                eq("AbbreviatedTaxInvoicePdfDocument"),
                eq("saga-123"),
                eq("saga.reply.abbreviated-tax-invoice-pdf"),
                eq("saga-123"),
                anyString());
    }

    @Test
    @DisplayName("publishSuccess() includes pdfUrl and pdfSize in reply")
    void testPublishSuccess_ReplyPayload() {
        sagaReplyPublisher.publishSuccess(
                "saga-1", SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF,
                "corr-1", "http://localhost:9000/abbreviatedtaxinvoices/test.pdf", 12345L);

        ArgumentCaptor<AbbreviatedTaxInvoicePdfReplyEvent> replyCaptor =
                ArgumentCaptor.forClass(AbbreviatedTaxInvoicePdfReplyEvent.class);
        verify(outboxService).saveWithRouting(
                replyCaptor.capture(), anyString(), anyString(), anyString(), anyString(), anyString());

        AbbreviatedTaxInvoicePdfReplyEvent reply = replyCaptor.getValue();
        assertThat(reply.isSuccess()).isTrue();
        assertThat(reply.getPdfUrl()).isEqualTo("http://localhost:9000/abbreviatedtaxinvoices/test.pdf");
        assertThat(reply.getPdfSize()).isEqualTo(12345L);
    }

    @Test
    @DisplayName("publishFailure() calls OutboxService with FAILURE reply")
    void testPublishFailure() {
        sagaReplyPublisher.publishFailure(
                "saga-123", SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF,
                "corr-456", "PDF generation failed");

        ArgumentCaptor<AbbreviatedTaxInvoicePdfReplyEvent> replyCaptor =
                ArgumentCaptor.forClass(AbbreviatedTaxInvoicePdfReplyEvent.class);
        verify(outboxService).saveWithRouting(
                replyCaptor.capture(), anyString(), anyString(), anyString(), anyString(), anyString());

        AbbreviatedTaxInvoicePdfReplyEvent reply = replyCaptor.getValue();
        assertThat(reply.isFailure()).isTrue();
        assertThat(reply.getErrorMessage()).isEqualTo("PDF generation failed");
    }

    @Test
    @DisplayName("publishCompensated() calls OutboxService with COMPENSATED reply")
    void testPublishCompensated() {
        sagaReplyPublisher.publishCompensated(
                "saga-123", SagaStep.GENERATE_ABBREVIATED_TAX_INVOICE_PDF, "corr-456");

        ArgumentCaptor<AbbreviatedTaxInvoicePdfReplyEvent> replyCaptor =
                ArgumentCaptor.forClass(AbbreviatedTaxInvoicePdfReplyEvent.class);
        verify(outboxService).saveWithRouting(
                replyCaptor.capture(), anyString(), anyString(), anyString(), anyString(), anyString());

        assertThat(replyCaptor.getValue().isCompensated()).isTrue();
    }
}
```

- [ ] **Step 7: Create `AbbreviatedTaxInvoicePdfGenerationServiceImplTest`**

Create `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/pdf/AbbreviatedTaxInvoicePdfGenerationServiceImplTest.java`:

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.pdf;

import com.wpanther.abbreviatedtaxinvoice.pdf.domain.service.AbbreviatedTaxInvoicePdfGenerationService.AbbreviatedTaxInvoicePdfGenerationException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("AbbreviatedTaxInvoicePdfGenerationServiceImpl Unit Tests")
class AbbreviatedTaxInvoicePdfGenerationServiceImplTest {

    @Mock
    private FopAbbreviatedTaxInvoicePdfGenerator fopPdfGenerator;

    @Mock
    private PdfA3Converter pdfA3Converter;

    private AbbreviatedTaxInvoicePdfGenerationServiceImpl service;

    @BeforeEach
    void setUp() throws Exception {
        service = new AbbreviatedTaxInvoicePdfGenerationServiceImpl(fopPdfGenerator, pdfA3Converter);
    }

    @Test
    @DisplayName("generatePdf() with null signedXml throws")
    void testGeneratePdf_nullSignedXml() {
        assertThatThrownBy(() -> service.generatePdf("ATINV-001", null))
                .isInstanceOf(AbbreviatedTaxInvoicePdfGenerationException.class)
                .hasMessageContaining("signedXml is null or blank");
    }

    @Test
    @DisplayName("generatePdf() with blank signedXml throws")
    void testGeneratePdf_blankSignedXml() {
        assertThatThrownBy(() -> service.generatePdf("ATINV-001", "   "))
                .isInstanceOf(AbbreviatedTaxInvoicePdfGenerationException.class)
                .hasMessageContaining("signedXml is null or blank");
    }

    @Test
    @DisplayName("generatePdf() with missing GrandTotalAmount throws")
    void testGeneratePdf_missingGrandTotal() throws Exception {
        String xml = "<?xml version=\"1.0\"?>" +
                "<rsm:AbbreviatedTaxInvoice_CrossIndustryInvoice " +
                "xmlns:rsm=\"urn:etda:uncefact:data:standard:AbbreviatedTaxInvoice_CrossIndustryInvoice:2\" " +
                "xmlns:ram=\"urn:etda:uncefact:data:standard:AbbreviatedTaxInvoice_ReusableAggregateBusinessInformationEntity:2\">" +
                "</rsm:AbbreviatedTaxInvoice_CrossIndustryInvoice>";

        assertThatThrownBy(() -> service.generatePdf("ATINV-001", xml))
                .isInstanceOf(AbbreviatedTaxInvoicePdfGenerationException.class)
                .hasMessageContaining("GrandTotalAmount not found");
    }

    @Test
    @DisplayName("generatePdf() with FOP failure wraps in domain exception")
    void testGeneratePdf_fopFailure() throws Exception {
        when(fopPdfGenerator.generatePdf(anyString(), anyMap()))
                .thenThrow(new FopAbbreviatedTaxInvoicePdfGenerator.PdfGenerationException("FOP error"));

        String xml = "<?xml version=\"1.0\"?>" +
                "<rsm:AbbreviatedTaxInvoice_CrossIndustryInvoice " +
                "xmlns:rsm=\"urn:etda:uncefact:data:standard:AbbreviatedTaxInvoice_CrossIndustryInvoice:2\" " +
                "xmlns:ram=\"urn:etda:uncefact:data:standard:AbbreviatedTaxInvoice_ReusableAggregateBusinessInformationEntity:2\">" +
                "<rsm:SupplyChainTradeTransaction>" +
                "<ram:ApplicableHeaderTradeSettlement>" +
                "<ram:SpecifiedTradeSettlementHeaderMonetarySummation>" +
                "<ram:GrandTotalAmount>1000</ram:GrandTotalAmount>" +
                "</ram:SpecifiedTradeSettlementHeaderMonetarySummation>" +
                "</ram:ApplicableHeaderTradeSettlement>" +
                "</rsm:SupplyChainTradeTransaction>" +
                "</rsm:AbbreviatedTaxInvoice_CrossIndustryInvoice>";

        assertThatThrownBy(() -> service.generatePdf("ATINV-001", xml))
                .isInstanceOf(AbbreviatedTaxInvoicePdfGenerationException.class)
                .hasMessageContaining("PDF generation failed");
    }
}
```

- [ ] **Step 8: Create `ThaiAmountWordsConverterTest`**

Create `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/infrastructure/adapter/out/pdf/ThaiAmountWordsConverterTest.java`:

```java
package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.pdf;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.*;

@DisplayName("ThaiAmountWordsConverter Unit Tests")
class ThaiAmountWordsConverterTest {

    @Test
    @DisplayName("Zero baht")
    void testZero() {
        assertThat(ThaiAmountWordsConverter.toWords(BigDecimal.ZERO))
                .isEqualTo("ศูนย์บาทถ้วน");
    }

    @Test
    @DisplayName("Integer amount with no satang")
    void testIntegerBaht() {
        assertThat(ThaiAmountWordsConverter.toWords(new BigDecimal("1000")))
                .contains("บาทถ้วน");
    }

    @Test
    @DisplayName("Amount with satang")
    void testBahtAndSatang() {
        assertThat(ThaiAmountWordsConverter.toWords(new BigDecimal("100.50")))
                .contains("บาท")
                .contains("สตางค์")
                .doesNotContain("ถ้วน");
    }

    @Test
    @DisplayName("Negative amount throws IllegalArgumentException")
    void testNegativeThrows() {
        assertThatThrownBy(() -> ThaiAmountWordsConverter.toWords(new BigDecimal("-1")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("Null amount throws IllegalArgumentException")
    void testNullThrows() {
        assertThatThrownBy(() -> ThaiAmountWordsConverter.toWords(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("Large amount with ล้าน (million)")
    void testMillionAmount() {
        String result = ThaiAmountWordsConverter.toWords(new BigDecimal("198628"));
        assertThat(result).contains("ล้าน");
    }
}
```

- [ ] **Step 9: Run all new tests**

```bash
cd /home/wpanther/projects/etax/invoice-microservices/services/abbreviatedtaxinvoice-pdf-generation-service
mvn test -Dtest="AbbreviatedTaxInvoicePdfDocumentTest,AbbreviatedTaxInvoicePdfGenerationExceptionTest,PdfGenerationConstantsTest,AbbreviatedTaxInvoicePdfDocumentRepositoryAdapterTest,EventPublisherTest,SagaReplyPublisherTest,AbbreviatedTaxInvoicePdfGenerationServiceImplTest,ThaiAmountWordsConverterTest" 2>&1 | tail -15
```

Expected: `BUILD SUCCESS`.

- [ ] **Step 10: Commit**

```bash
git add src/
git commit -m "feat: add remaining domain, persistence, messaging, and PDF service tests"
```

---

## Task 17: Full Compile + Test Suite Verification + PDF Preview Test

**Files:**
- Create: `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/PdfPreviewTest.java`
- Create: `src/test/resources/xml/preview-abbreviatedtaxinvoice.xml`

- [ ] **Step 1: Create preview XML fixture**

Create `src/test/resources/xml/preview-abbreviatedtaxinvoice.xml` using a realistic AbbreviatedTaxInvoice document (seller-only, no buyer):

```xml
<?xml version="1.0" encoding="UTF-8"?>
<rsm:AbbreviatedTaxInvoice_CrossIndustryInvoice
    xmlns:ram="urn:etda:uncefact:data:standard:AbbreviatedTaxInvoice_ReusableAggregateBusinessInformationEntity:2"
    xmlns:rsm="urn:etda:uncefact:data:standard:AbbreviatedTaxInvoice_CrossIndustryInvoice:2">
  <rsm:ExchangedDocument>
    <ram:ID>ATINV-2568-0042</ram:ID>
    <ram:Name>ใบกำกับภาษีอย่างย่อ</ram:Name>
    <ram:TypeCode>T05</ram:TypeCode>
    <ram:IssueDateTime>2025-06-15T10:30:00.0</ram:IssueDateTime>
  </rsm:ExchangedDocument>
  <rsm:SupplyChainTradeTransaction>
    <ram:ApplicableHeaderTradeAgreement>
      <ram:SellerTradeParty>
        <ram:Name>บริษัท ตัวอย่าง จำกัด</ram:Name>
        <ram:SpecifiedTaxRegistration>
          <ram:ID>0123456789012</ram:ID>
        </ram:SpecifiedTaxRegistration>
        <ram:PostalTradeAddress>
          <ram:LineOne>123 ถนนสุขุมวิท</ram:LineOne>
          <ram:CityName>กรุงเทพมหานคร</ram:CityName>
          <ram:PostcodeCode>10110</ram:PostcodeCode>
        </ram:PostalTradeAddress>
      </ram:SellerTradeParty>
    </ram:ApplicableHeaderTradeAgreement>
    <ram:ApplicableHeaderTradeDelivery/>
    <ram:ApplicableHeaderTradeSettlement>
      <ram:InvoiceCurrencyCode>THB</ram:InvoiceCurrencyCode>
      <ram:ApplicableTradeTax>
        <ram:TypeCode>VAT</ram:TypeCode>
        <ram:CalculatedRate>7</ram:CalculatedRate>
      </ram:ApplicableTradeTax>
      <ram:SpecifiedTradeSettlementHeaderMonetarySummation>
        <ram:LineTotalAmount>5000.00</ram:LineTotalAmount>
        <ram:AllowanceTotalAmount>0.00</ram:AllowanceTotalAmount>
        <ram:TaxBasisTotalAmount>5000.00</ram:TaxBasisTotalAmount>
        <ram:TaxTotalAmount>350.00</ram:TaxTotalAmount>
        <ram:GrandTotalAmount>5350.00</ram:GrandTotalAmount>
      </ram:SpecifiedTradeSettlementHeaderMonetarySummation>
    </ram:ApplicableHeaderTradeSettlement>
    <ram:IncludedSupplyChainTradeLineItem>
      <ram:AssociatedDocumentLineDocument>
        <ram:LineID>1</ram:LineID>
      </ram:AssociatedDocumentLineDocument>
      <ram:SpecifiedTradeProduct>
        <ram:ID>P001</ram:ID>
        <ram:Name>สินค้าตัวอย่าง</ram:Name>
      </ram:SpecifiedTradeProduct>
      <ram:SpecifiedLineTradeAgreement>
        <ram:GrossPriceProductTradePrice>
          <ram:ChargeAmount>5000.00</ram:ChargeAmount>
        </ram:GrossPriceProductTradePrice>
      </ram:SpecifiedLineTradeAgreement>
      <ram:SpecifiedLineTradeDelivery>
        <ram:BilledQuantity unitCode="PIECE">1</ram:BilledQuantity>
      </ram:SpecifiedLineTradeDelivery>
      <ram:SpecifiedLineTradeSettlement>
        <ram:ApplicableTradeTax>
          <ram:TypeCode>VAT</ram:TypeCode>
          <ram:CalculatedRate>7</ram:CalculatedRate>
        </ram:ApplicableTradeTax>
        <ram:SpecifiedTradeSettlementLineMonetarySummation>
          <ram:NetLineTotalAmount>5000.00</ram:NetLineTotalAmount>
        </ram:SpecifiedTradeSettlementLineMonetarySummation>
      </ram:SpecifiedLineTradeSettlement>
    </ram:IncludedSupplyChainTradeLineItem>
  </rsm:SupplyChainTradeTransaction>
</rsm:AbbreviatedTaxInvoice_CrossIndustryInvoice>
```

- [ ] **Step 2: Create `PdfPreviewTest`**

Create `src/test/java/com/wpanther/abbreviatedtaxinvoice/pdf/PdfPreviewTest.java`:

```java
package com.wpanther.abbreviatedtaxinvoice.pdf;

import com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.pdf.PdfA3Converter;
import com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.pdf.ThaiAmountWordsConverter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.fop.apps.FopFactory;
import org.apache.fop.apps.MimeConstants;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;

import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Templates;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXResult;
import javax.xml.transform.stream.StreamSource;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("PDF/A-3 Preview Generator")
class PdfPreviewTest {

    private static final Path OUTPUT_DIR = Path.of("target/preview");
    private static final BigDecimal GRAND_TOTAL = new BigDecimal("5350.00");
    private static final String DOC_NUMBER = "ATINV-2568-0042";

    private static String signedXml;
    private static SimpleMeterRegistry registry;

    @BeforeAll
    static void loadFixtures() throws Exception {
        ClassPathResource xmlResource = new ClassPathResource("xml/preview-abbreviatedtaxinvoice.xml");
        try (InputStream is = xmlResource.getInputStream()) {
            signedXml = new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
        Files.createDirectories(OUTPUT_DIR);
        registry = new SimpleMeterRegistry();
    }

    @Test
    @DisplayName("Full pipeline: signed XML → FOP → PDF/A-3 with embedded XML")
    void generatePdfA3Preview() throws Exception {
        String amountInWords = ThaiAmountWordsConverter.toWords(GRAND_TOTAL);
        assertThat(amountInWords).isEqualTo("ห้าพันสามร้อยห้าสิบบาทถ้วน");

        // Step 1: Render base PDF
        byte[] basePdf = renderBasePdf(amountInWords);
        assertThat(basePdf).isNotEmpty();
        assertStartsWithPdfHeader(basePdf);

        // Step 2: Convert to PDF/A-3 with embedded signed XML
        PdfA3Converter converter = new PdfA3Converter("icc/sRGB.icc", registry);
        String xmlFilename = "abbreviated-tax-invoice-" + DOC_NUMBER + ".xml";
        byte[] pdfA3 = converter.convertToPdfA3(basePdf, signedXml, xmlFilename, DOC_NUMBER);

        assertThat(pdfA3).isNotEmpty();
        assertThat(pdfA3.length).isGreaterThan(basePdf.length);
        assertStartsWithPdfHeader(pdfA3);

        // Save for visual inspection
        Path outputPath = OUTPUT_DIR.resolve("abbreviated-tax-invoice-pdfa3-preview.pdf");
        Files.write(outputPath, pdfA3);
        System.out.println("PDF/A-3 saved: " + outputPath.toAbsolutePath() + " (" + pdfA3.length + " bytes)");
    }

    private byte[] renderBasePdf(String amountInWords) throws Exception {
        FopFactory fopFactory = createProductionFopFactory();
        Templates templates = compileTemplates();

        try (ByteArrayOutputStream pdfOutput = new ByteArrayOutputStream()) {
            var fop = fopFactory.newFop(MimeConstants.MIME_PDF, pdfOutput);
            Transformer transformer = templates.newTransformer();
            transformer.setParameter("amountInWords", amountInWords);

            Source xmlSource = new StreamSource(
                    new ByteArrayInputStream(signedXml.getBytes(StandardCharsets.UTF_8)));
            Result result = new SAXResult(fop.getDefaultHandler());
            transformer.transform(xmlSource, result);

            return pdfOutput.toByteArray();
        }
    }

    private FopFactory createProductionFopFactory() throws Exception {
        Path projectRoot = Path.of("").toAbsolutePath();
        Path prodConfig = projectRoot.resolve("src/main/resources/fop/fop.xconf");
        try (InputStream is = Files.newInputStream(prodConfig)) {
            return FopFactory.newInstance(prodConfig.getParent().toUri(), is);
        }
    }

    private Templates compileTemplates() throws Exception {
        ClassPathResource xslResource =
                new ClassPathResource("xsl/abbreviatedtaxinvoice-direct.xsl");
        try (InputStream is = xslResource.getInputStream()) {
            return TransformerFactory.newInstance().newTemplates(new StreamSource(is));
        }
    }

    private static void assertStartsWithPdfHeader(byte[] pdfBytes) {
        assertThat(new String(pdfBytes, 0, 4, StandardCharsets.US_ASCII))
                .isEqualTo("%PDF");
    }
}
```

- [ ] **Step 3: Run PDF preview test**

```bash
cd /home/wpanther/projects/etax/invoice-microservices/services/abbreviatedtaxinvoice-pdf-generation-service
mvn test -Dtest="PdfPreviewTest" 2>&1 | tail -10
```

Expected: `BUILD SUCCESS`. The PDF is saved to `target/preview/abbreviated-tax-invoice-pdfa3-preview.pdf` for visual inspection.

- [ ] **Step 4: Run full test suite with coverage**

```bash
mvn verify 2>&1 | tail -25
```

Expected: `BUILD SUCCESS` with all tests passing and JaCoCo coverage ≥ 90% per package.

If JaCoCo fails on a specific package, check the HTML report at `target/site/jacoco/index.html` and add tests for uncovered branches.

- [ ] **Step 5: Final full compile**

```bash
mvn compile -DskipTests 2>&1 | tail -5
```

Expected: `BUILD SUCCESS`.

- [ ] **Step 6: Commit**

```bash
git add src/
git commit -m "feat: add PDF preview test and XML fixture; full test suite verification"
```

---

## Implementation Plan — Complete

All 17 tasks cover the full service implementation:

| Task | Description |
|------|-------------|
| 1 | Saga Commons — `GENERATE_ABBREVIATED_TAX_INVOICE_PDF` enum |
| 2 | Maven project scaffold — `pom.xml` + main application class |
| 3 | Domain layer — exception, constants, `GenerationStatus`, repository/service interfaces |
| 4 | Domain model — `AbbreviatedTaxInvoicePdfDocument` aggregate root |
| 5 | Application ports + use case interfaces |
| 6 | Infrastructure persistence — entity, JPA repository, adapter |
| 7 | Infrastructure persistence — outbox + Flyway migration |
| 8 | Infrastructure messaging — events + publishers |
| 9 | Kafka commands, mapper, and Camel route config |
| 10 | Configuration classes + metrics |
| 11 | PDF infrastructure — FOP generator, PDF/A-3 converter, amount words, service impl |
| 12 | XSL-FO template + FOP/ICC/font resources |
| 13 | MinIO storage adapter + cleanup service |
| 14 | REST client + application services (DocumentService + SagaCommandHandler) |
| 15 | `application.yml` + test config + Camel route serialization tests + command mapper tests |
| 16 | Remaining domain/persistence/messaging/PDF tests |
| 17 | Full compile + test suite verification + PDF preview test |
