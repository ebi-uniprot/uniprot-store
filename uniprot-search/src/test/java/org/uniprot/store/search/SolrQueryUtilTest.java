package org.uniprot.store.search;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;

import org.junit.jupiter.api.Test;

/** @author lgonzales */
class SolrQueryUtilTest {
    @Test
    void hasFieldTermsWithValidTermsReturnTrue() {
        String inputQuery = "organism:human";
        boolean hasFieldTerm = SolrQueryUtil.hasFieldTerms(inputQuery, "organism");
        assertTrue(hasFieldTerm);
    }

    @Test
    void hasFieldTermsWithInvalidTermsReturnFalse() {
        String inputQuery = "organism:human";
        boolean hasFieldTerm = SolrQueryUtil.hasFieldTerms(inputQuery, "invalid");
        assertFalse(hasFieldTerm);
    }

    @Test
    void hasFieldTermsWithValidPhraseTermsReturnTrue() {
        String inputQuery = "organism:\"homo sapiens\"";
        boolean hasFieldTerm = SolrQueryUtil.hasFieldTerms(inputQuery, "organism");
        assertTrue(hasFieldTerm);
    }

    @Test
    void hasFieldTermsWithInvalidPhraseTermsReturnFalse() {
        String inputQuery = "organism:\"homo sapiens\"";
        boolean hasFieldTerm = SolrQueryUtil.hasFieldTerms(inputQuery, "invalid");
        assertFalse(hasFieldTerm);
    }

    @Test
    void hasFieldTermsWithValidRangeTermsReturnTrue() {
        String inputQuery = "length:[1 TO 10}";
        boolean hasFieldTerm = SolrQueryUtil.hasFieldTerms(inputQuery, "length");
        assertTrue(hasFieldTerm);
    }

    @Test
    void hasFieldTermsWithValidWildcardQueryTermsReturnTrue() {
        String inputQuery = "organism:*";
        boolean hasFieldTerm = SolrQueryUtil.hasFieldTerms(inputQuery, "organism");
        assertTrue(hasFieldTerm);
    }

    @Test
    void hasFieldTermsWithInvalidRangeTermsReturnFalse() {
        String inputQuery = "length:[1 TO 10}";
        boolean hasFieldTerm = SolrQueryUtil.hasFieldTerms(inputQuery, "invalid");
        assertFalse(hasFieldTerm);
    }

    @Test
    void hasFieldTermsWithValidBooleanQueryReturnTrue() {
        String inputQuery = "(organism:human) AND (length:[1 TO 10})";
        boolean hasFieldTerm = SolrQueryUtil.hasFieldTerms(inputQuery, "length", "organism");
        assertTrue(hasFieldTerm);
    }

    @Test
    void hasFieldTermsWithInvalidBooleanQueryReturnFalse() {
        String inputQuery = "(organism:human) AND (length:[1 TO 10})";
        boolean hasFieldTerm = SolrQueryUtil.hasFieldTerms(inputQuery, "invalid2", "invalid");
        assertFalse(hasFieldTerm);
    }

    @Test
    void hasFieldTermsWithOneValidBooleanQueryReturnTrue() {
        String inputQuery = "(organism:human) AND (length:[1 TO 10})";
        boolean hasFieldTerm = SolrQueryUtil.hasFieldTerms(inputQuery, "length", "invalid");
        assertTrue(hasFieldTerm);
    }

    @Test
    void getTermValuesManyValues() {
        String inputQuery = "(organism:\"homo sapiens\") OR (organism:rat)";
        List<String> values = SolrQueryUtil.getTermValues(inputQuery, "organism");
        assertNotNull(values);
        assertEquals(2, values.size());
        assertEquals("homo sapiens", values.get(0));
        assertEquals("rat", values.get(1));
    }

    @Test
    void getTermValuesOneValue() {
        String inputQuery = "(organism:human) OR (length:[1 TO 10})";
        List<String> values = SolrQueryUtil.getTermValues(inputQuery, "organism");
        assertNotNull(values);
        assertEquals(1, values.size());
        assertEquals("human", values.get(0));
    }

    @Test
    void getTermValuesNoValue() {
        String inputQuery = "organism:*";
        List<String> values = SolrQueryUtil.getTermValues(inputQuery, "keyword");
        assertNotNull(values);
        assertTrue(values.isEmpty());
    }

    @Test
    void getTermValue() {
        String inputQuery = "length:[1 TO 10]";
        String value = SolrQueryUtil.getTermValue(inputQuery, "length");
        assertNotNull(value);
        assertEquals("[1 TO 10]", value);
    }

    @Test
    void hasNegativeTermSuccess() {
        String inputQuery = "NOT (organism:Human)";
        boolean result = SolrQueryUtil.hasNegativeTerm(inputQuery);
        assertTrue(result);
    }

    @Test
    void hasNegativeTermWithoutBracketsSuccess() {
        String inputQuery = "NOT organism:Human";
        boolean result = SolrQueryUtil.hasNegativeTerm(inputQuery);
        assertTrue(result);
    }

    @Test
    void hasNegativeTermComplexQuerySuccess() {
        String inputQuery = "NOT (organism:Human) OR accession:P21802";
        boolean result = SolrQueryUtil.hasNegativeTerm(inputQuery);
        assertTrue(result);
    }

    @Test
    void hasNegativeTermReturnsFalse() {
        String inputQuery = "organism:Human";
        boolean result = SolrQueryUtil.hasNegativeTerm(inputQuery);
        assertFalse(result);
    }

    @Test
    void hasNegativeTermComplexQueryReturnsFalse() {
        String inputQuery = "organism:Human OR accession:P21802";
        boolean result = SolrQueryUtil.hasNegativeTerm(inputQuery);
        assertFalse(result);
    }
}
