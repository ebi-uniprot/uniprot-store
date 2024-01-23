package org.uniprot.store.search.field.validator;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class FieldRegexConstantsTest {

    @Test
    void testUniProtKBAccessionOrIdRegexWithVersion() {
        assertTrue("P21802".matches(FieldRegexConstants.UNIPROTKB_ACCESSION_OR_ID));
        assertTrue("P21802.2".matches(FieldRegexConstants.UNIPROTKB_ACCESSION_OR_ID));
        assertTrue("P21802.20".matches(FieldRegexConstants.UNIPROTKB_ACCESSION_OR_ID));
        assertTrue("P21802.222".matches(FieldRegexConstants.UNIPROTKB_ACCESSION_OR_ID));

        assertFalse("P21802.2222".matches(FieldRegexConstants.UNIPROTKB_ACCESSION_OR_ID));
        assertFalse("P21802.".matches(FieldRegexConstants.UNIPROTKB_ACCESSION_OR_ID));
        assertFalse("P21802.XX".matches(FieldRegexConstants.UNIPROTKB_ACCESSION_OR_ID));
    }

    @Test
    void testUniProtKBAccessionOrIdRegexWithIsoform() {
        assertTrue("P21802-1".matches(FieldRegexConstants.UNIPROTKB_ACCESSION_OR_ID));
        assertTrue("P21802-20".matches(FieldRegexConstants.UNIPROTKB_ACCESSION_OR_ID));

        assertFalse("P21802-".matches(FieldRegexConstants.UNIPROTKB_ACCESSION_OR_ID));
        assertFalse("P21802-XXXX".matches(FieldRegexConstants.UNIPROTKB_ACCESSION_OR_ID));
    }

    @Test
    void testUniProtKBAccessionOrIdRegexWithTremblId() {
        assertTrue("B5T0J4_9HIV1".matches(FieldRegexConstants.UNIPROTKB_ACCESSION_OR_ID));
        assertTrue("I7F3J9_9HIV1".matches(FieldRegexConstants.UNIPROTKB_ACCESSION_OR_ID));
        assertTrue("A7LLU3_ORYSJ".matches(FieldRegexConstants.UNIPROTKB_ACCESSION_OR_ID));
        assertTrue("A0A2P1AB45_9HIV1".matches(FieldRegexConstants.UNIPROTKB_ACCESSION_OR_ID));

        assertFalse("XP12345_9HIV1".matches(FieldRegexConstants.UNIPROTKB_ACCESSION_OR_ID));
        assertFalse("T12345_9HIV1".matches(FieldRegexConstants.UNIPROTKB_ACCESSION_OR_ID));
        assertFalse("A0A2P1AB45_".matches(FieldRegexConstants.UNIPROTKB_ACCESSION_OR_ID));
        assertFalse("A0A2P1AB45_1".matches(FieldRegexConstants.UNIPROTKB_ACCESSION_OR_ID));
        assertFalse("A0A2P1AB45_1234567".matches(FieldRegexConstants.UNIPROTKB_ACCESSION_OR_ID));
    }

    @Test
    void testUniProtKBAccessionOrIdRegexWithSwissId() {
        assertTrue("PPNP_BURCJ".matches(FieldRegexConstants.UNIPROTKB_ACCESSION_OR_ID));
        assertTrue("POLG_WMV2A".matches(FieldRegexConstants.UNIPROTKB_ACCESSION_OR_ID));
        assertTrue("PHOSP_HRSVA".matches(FieldRegexConstants.UNIPROTKB_ACCESSION_OR_ID));

        assertFalse("A_9HIV1".matches(FieldRegexConstants.UNIPROTKB_ACCESSION_OR_ID));
        assertFalse("123456_9HIV1".matches(FieldRegexConstants.UNIPROTKB_ACCESSION_OR_ID));
        assertFalse("PHOSP_".matches(FieldRegexConstants.UNIPROTKB_ACCESSION_OR_ID));
        assertFalse("PHOSP_1".matches(FieldRegexConstants.UNIPROTKB_ACCESSION_OR_ID));
    }

    @Test
    void testECIdRegex() {
        assertTrue("1.-.-.-".matches(FieldRegexConstants.EC_ID_REGEX));
        assertTrue("1.22.-.-".matches(FieldRegexConstants.EC_ID_REGEX));
        assertTrue("1.12.1.-".matches(FieldRegexConstants.EC_ID_REGEX));
        assertTrue("1.12.24.n1".matches(FieldRegexConstants.EC_ID_REGEX));

        assertFalse("8.-.-.-".matches(FieldRegexConstants.EC_ID_REGEX));
        assertFalse("10.-.-.-".matches(FieldRegexConstants.EC_ID_REGEX));
        assertFalse("1.-.1.-".matches(FieldRegexConstants.EC_ID_REGEX));
        assertFalse("1.n2.-.-".matches(FieldRegexConstants.EC_ID_REGEX));
        assertFalse("".matches(FieldRegexConstants.EC_ID_REGEX));
    }

    @Test
    void testGOIdRegex() {
        assertTrue("GO:1234567".matches(FieldRegexConstants.GO_ID_REGEX));
        assertTrue("GO:9234567".matches(FieldRegexConstants.GO_ID_REGEX));

        assertFalse("GO;1234567".matches(FieldRegexConstants.GO_ID_REGEX));
        assertFalse("1234567".matches(FieldRegexConstants.GO_ID_REGEX));
        assertFalse("GO:12345678".matches(FieldRegexConstants.GO_ID_REGEX));
        assertFalse("GO:1234".matches(FieldRegexConstants.GO_ID_REGEX));
        assertFalse("GO:ASD1234".matches(FieldRegexConstants.GO_ID_REGEX));
        assertFalse("".matches(FieldRegexConstants.GO_ID_REGEX));
    }

    @Test
    void testKeywordIdRegex() {
        assertTrue("KW-1234".matches(FieldRegexConstants.KEYWORD_ID_REGEX));
        assertTrue("KW-4321".matches(FieldRegexConstants.KEYWORD_ID_REGEX));

        assertFalse("KW:1234".matches(FieldRegexConstants.KEYWORD_ID_REGEX));
        assertFalse("KW-K123".matches(FieldRegexConstants.KEYWORD_ID_REGEX));
        assertFalse("KW-12345".matches(FieldRegexConstants.KEYWORD_ID_REGEX));
        assertFalse("1234".matches(FieldRegexConstants.KEYWORD_ID_REGEX));
        assertFalse("".matches(FieldRegexConstants.KEYWORD_ID_REGEX));
    }

    @Test
    void testTaxonomyIdRegex() {
        assertTrue("1".matches(FieldRegexConstants.TAXONOMY_ID_REGEX));
        assertTrue("123".matches(FieldRegexConstants.TAXONOMY_ID_REGEX));
        assertTrue("12345677".matches(FieldRegexConstants.TAXONOMY_ID_REGEX));

        assertFalse("".matches(FieldRegexConstants.TAXONOMY_ID_REGEX));
        assertFalse("HP".matches(FieldRegexConstants.TAXONOMY_ID_REGEX));
        assertFalse("HP123".matches(FieldRegexConstants.TAXONOMY_ID_REGEX));
        assertFalse("123~".matches(FieldRegexConstants.TAXONOMY_ID_REGEX));
    }

    @Test
    void testUniProtKBAccessionWithSequence() {
        assertTrue("P21802[10-20]".matches(FieldRegexConstants.UNIPROTKB_ACCESSION_OR_ID));
        assertTrue("P21802[20-30]".matches(FieldRegexConstants.UNIPROTKB_ACCESSION_OR_ID));
        assertTrue("P21802-1[10-20]".matches(FieldRegexConstants.UNIPROTKB_ACCESSION_OR_ID));
        assertFalse("P21802-[10-20]".matches(FieldRegexConstants.UNIPROTKB_ACCESSION_OR_ID));
        assertFalse("P21802[10-]".matches(FieldRegexConstants.UNIPROTKB_ACCESSION_OR_ID));
        assertFalse("P21802[-20]".matches(FieldRegexConstants.UNIPROTKB_ACCESSION_OR_ID));
        assertFalse("P21802.3[10-20]".matches(FieldRegexConstants.UNIPROTKB_ACCESSION_OR_ID));
        assertFalse("P21802.3[-20]".matches(FieldRegexConstants.UNIPROTKB_ACCESSION_OR_ID));
        assertFalse("P21802[10-20].3".matches(FieldRegexConstants.UNIPROTKB_ACCESSION_OR_ID));
        assertFalse("P21802[10-].3".matches(FieldRegexConstants.UNIPROTKB_ACCESSION_OR_ID));
        assertFalse(
                "A0A2P1AB45_9HIV1[10-20]".matches(FieldRegexConstants.UNIPROTKB_ACCESSION_OR_ID));
        assertFalse("PHOSP_1[10-20]".matches(FieldRegexConstants.UNIPROTKB_ACCESSION_OR_ID));
        // Need to validate this kind of invalid range in application code
        assertTrue("P21802[40-30]".matches(FieldRegexConstants.UNIPROTKB_ACCESSION_OR_ID));
    }
}
