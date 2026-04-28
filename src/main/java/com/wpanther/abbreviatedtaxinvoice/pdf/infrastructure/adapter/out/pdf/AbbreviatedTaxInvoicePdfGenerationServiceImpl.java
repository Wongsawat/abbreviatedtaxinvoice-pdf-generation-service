package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.pdf;

import com.wpanther.abbreviatedtaxinvoice.pdf.domain.service.AbbreviatedTaxInvoicePdfGenerationService;
import com.wpanther.abbreviatedtaxinvoice.pdf.domain.service.ThaiAmountWordsConverter;
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
        "urn:oasis:names:specification:ubl:schema:xsd:AbbreviatedTaxInvoice-2";
    private static final String RAM_NS =
        "urn:un:unece:uncefact:data:standard:ReusableAggregateBusinessInformationEntity:100";
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

    public AbbreviatedTaxInvoicePdfGenerationServiceImpl(FopAbbreviatedTaxInvoicePdfGenerator fopPdfGenerator,
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

            String xmlFilename = "abbreviatedtaxinvoice-" + abbreviatedTaxInvoiceNumber + ".xml";
            byte[] pdfA3 = pdfA3Converter.convertToPdfA3(basePdf, signedXml, xmlFilename, abbreviatedTaxInvoiceNumber);
            log.info("Generated PDF/A-3 for abbreviated tax invoice {}: {} bytes", abbreviatedTaxInvoiceNumber, pdfA3.length);
            return pdfA3;

        } catch (FopAbbreviatedTaxInvoicePdfGenerator.PdfGenerationException e) {
            log.error("FOP PDF generation failed for abbreviated tax invoice: {}", abbreviatedTaxInvoiceNumber, e);
            throw new AbbreviatedTaxInvoicePdfGenerationException("PDF generation failed: " + e.getMessage(), e);
        } catch (PdfA3Converter.PdfConversionException e) {
            log.error("PDF/A-3 conversion failed for abbreviated tax invoice: {}", abbreviatedTaxInvoiceNumber, e);
            throw new AbbreviatedTaxInvoicePdfGenerationException("PDF/A-3 conversion failed: " + e.getMessage(), e);
        } catch (AbbreviatedTaxInvoicePdfGenerationException e) {
            throw e;
        } catch (Exception e) {
            log.error("Unexpected error during PDF generation for abbreviated tax invoice: {}", abbreviatedTaxInvoiceNumber, e);
            throw new AbbreviatedTaxInvoicePdfGenerationException("PDF generation failed: " + e.getMessage(), e);
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
                    "GrandTotalAmount not found in signed XML for abbreviated tax invoice: " + abbreviatedTaxInvoiceNumber);
            }
            return new BigDecimal(value.trim());
        } catch (XPathExpressionException e) {
            throw new AbbreviatedTaxInvoicePdfGenerationException(
                "Failed to extract GrandTotalAmount from signed XML: " + e.getMessage(), e);
        } catch (NumberFormatException e) {
            throw new AbbreviatedTaxInvoicePdfGenerationException(
                "Invalid GrandTotalAmount in signed XML for abbreviated tax invoice " + abbreviatedTaxInvoiceNumber + ": " + e.getMessage(), e);
        }
    }
}