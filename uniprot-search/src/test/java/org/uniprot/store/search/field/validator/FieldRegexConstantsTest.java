package org.uniprot.store.search.field.validator;

import static org.junit.jupiter.api.Assertions.*;

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
}
