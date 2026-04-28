package com.wpanther.abbreviatedtaxinvoice.pdf.infrastructure.adapter.out.pdf;

import com.wpanther.abbreviatedtaxinvoice.pdf.domain.service.ThaiAmountWordsConverter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("ThaiAmountWordsConverter Tests")
class ThaiAmountWordsConverterTest {

    @Test
    @DisplayName("convert 0 baht to ศูนย์บาทถ้วน")
    void testZeroBaht() {
        String result = ThaiAmountWordsConverter.toWords(BigDecimal.ZERO);
        assertThat(result).isEqualTo("ศูนย์บาทถ้วน");
    }

    @Test
    @DisplayName("convert 1 baht to หนึ่งบาทถ้วน")
    void testOneBaht() {
        String result = ThaiAmountWordsConverter.toWords(BigDecimal.ONE);
        assertThat(result).isEqualTo("หนึ่งบาทถ้วน");
    }

    @Test
    @DisplayName("convert 2 baht correctly")
    void testTwoBaht() {
        String result = ThaiAmountWordsConverter.toWords(new BigDecimal("2"));
        assertThat(result).isEqualTo("สองบาทถ้วน");
    }

    @Test
    @DisplayName("convert 10 baht to สิบบาทถ้วน")
    void testTenBaht() {
        String result = ThaiAmountWordsConverter.toWords(new BigDecimal("10"));
        assertThat(result).isEqualTo("สิบบาทถ้วน");
    }

    @Test
    @DisplayName("convert 11 baht with สิบเอ็ด")
    void testElevenBaht() {
        String result = ThaiAmountWordsConverter.toWords(new BigDecimal("11"));
        assertThat(result).isEqualTo("สิบเอ็ดบาทถ้วน");
    }

    @Test
    @DisplayName("convert 21 baht with ยี่สิบ")
    void testTwentyOneBaht() {
        String result = ThaiAmountWordsConverter.toWords(new BigDecimal("21"));
        assertThat(result).isEqualTo("ยี่สิบเอ็ดบาทถ้วน");
    }

    @ParameterizedTest
    @CsvSource({
        "100,หนึ่งร้อยบาทถ้วน",
        "1000,หนึ่งพันบาทถ้วน",
        "1000000,หนึ่งล้านบาทถ้วน",
        "1234567,หนึ่งล้านสองแสนสามหมื่นสี่พันห้าร้อยหกสิบเจ็ดบาทถ้วน"
    })
    @DisplayName("convert large amounts correctly")
    void testLargeAmounts(String input, String expected) {
        String result = ThaiAmountWordsConverter.toWords(new BigDecimal(input));
        assertThat(result).isEqualTo(expected);
    }

    @Test
    @DisplayName("convert amount with satang")
    void testWithSatang() {
        String result = ThaiAmountWordsConverter.toWords(new BigDecimal("125.50"));
        assertThat(result).isEqualTo("หนึ่งร้อยยี่สิบห้าบาทห้าสิบสตางค์");
    }

    @Test
    @DisplayName("convert 0.50 satang")
    void testFiftySatang() {
        String result = ThaiAmountWordsConverter.toWords(new BigDecimal("0.50"));
        assertThat(result).isEqualTo("ศูนย์บาทห้าสิบสตางค์");
    }

    @Test
    @DisplayName("null amount throws exception")
    void testNullAmount() {
        assertThatThrownBy(() -> ThaiAmountWordsConverter.toWords(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-negative");
    }

    @Test
    @DisplayName("negative amount throws exception")
    void testNegativeAmount() {
        assertThatThrownBy(() -> ThaiAmountWordsConverter.toWords(new BigDecimal("-1")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-negative");
    }
}