package uk.ac.ebi.uniprot.search;

import org.junit.jupiter.api.Test;
import uk.ac.ebi.uniprot.search.expr.BoostExpression;
import uk.ac.ebi.uniprot.search.field.SearchField;
import uk.ac.ebi.uniprot.search.field.SearchFieldType;
import uk.ac.ebi.uniprot.search.field.validator.FieldValueValidator;

import java.util.List;
import java.util.function.Predicate;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class DefaultSearchHandlerTest {
    private final DefaultSearchHandler defaultSearchHandler =
            new DefaultSearchHandler(FakeSearchField.content,
                                     FakeSearchField.accession,
                                     FakeSearchField.getBoostFields());

    @Test
    void hasDefaultSearchSimpleTermReturnTrue() {
        String defaultQuery = "human";
        boolean hasDefaultSearch = defaultSearchHandler.hasDefaultSearch(defaultQuery);
        assertTrue(hasDefaultSearch);
    }

    @Test
    void hasDefaultSearchWithoutDefaultReturnFalse() {
        String defaultQuery = "organism:human";
        boolean hasDefaultSearch = defaultSearchHandler.hasDefaultSearch(defaultQuery);
        assertFalse(hasDefaultSearch);
    }

    @Test
    void hasDefaultSearchWithManyTermsReturnTrue() {
        String defaultQuery = "organism:human AND default";
        boolean hasDefaultSearch = defaultSearchHandler.hasDefaultSearch(defaultQuery);
        assertTrue(hasDefaultSearch);
    }

    @Test
    void hasDefaultSearchWithManyTermsValueReturnTrue() {
        String defaultQuery = "organism:homo sapiens";
        boolean hasDefaultSearch = defaultSearchHandler.hasDefaultSearch(defaultQuery);
        assertTrue(hasDefaultSearch);
    }

    @Test
    void rewriteDefaultSearchTermQuery() {
        String defaultQuery = "human";
        String result = defaultSearchHandler.optimiseDefaultSearch(defaultQuery);
        assertNotNull(result);
        assertEquals("content:human (taxonomy_name:human)^2.0 (gene:human)^2.0", result);
    }

    @Test
    void rewriteDefaultSearchMultipleTermQuery() {
        String defaultQuery = "P53 9606";
        String result = defaultSearchHandler.optimiseDefaultSearch(defaultQuery);
        assertNotNull(result);
        String expectedResult = "+(content:P53 (taxonomy_name:P53)^2.0 (gene:P53)^2.0) " +
                "+(content:9606 (taxonomy_name:9606)^2.0 (taxonomy_id:9606)^2.0 (gene:9606)^2.0)";
        assertEquals(expectedResult, result);
    }

    @Test
    void rewriteDefaultSearchWithValidIdTermQuery() {
        String defaultQuery = "P21802";
        String result = defaultSearchHandler.optimiseDefaultSearch(defaultQuery);
        assertNotNull(result);
        String expectedResult = "accession:P21802";
        assertEquals(expectedResult, result);
    }

    @Test
    void rewriteDefaultSearchWithOthersTermsQuery() {
        String defaultQuery = "organism:9606 human";
        String result = defaultSearchHandler.optimiseDefaultSearch(defaultQuery);
        assertNotNull(result);
        assertEquals("+organism:9606 +(content:human (taxonomy_name:human)^2.0 (gene:human)^2.0)", result);
    }

    @Test
    void rewriteDefaultSearchPharseQuery() {
        String defaultQuery = "\"Homo Sapiens\"";
        String result = defaultSearchHandler.optimiseDefaultSearch(defaultQuery);
        assertNotNull(result);
        String expectedResult = "content:\"Homo Sapiens\" (taxonomy_name:\"Homo Sapiens\")^2.0 (gene:\"Homo Sapiens\")^2.0";
        assertEquals(expectedResult, result);
    }

    @Test
    void rewriteDefaultSearchPharseQueryAndOthersTerms() {
        String defaultQuery = "organism:9606 \"homo sapiens\"";
        String result = defaultSearchHandler.optimiseDefaultSearch(defaultQuery);
        assertNotNull(result);
        String expectedResult = "+organism:9606 " +
                "+(content:\"homo sapiens\" (taxonomy_name:\"homo sapiens\")^2.0 (gene:\"homo sapiens\")^2.0)";
        assertEquals(expectedResult, result);
    }

    @Test
    void appendSingleBoostExpression() {
        String expression = "myExpression:true";
        DefaultSearchHandler handler = new DefaultSearchHandler(
                FakeSearchField.content,
                FakeSearchField.accession,
                FakeSearchField.getBoostFields(),
                () -> singletonList(BoostExpression.builder()
                                             .expression(expression)
                                             .boost(4.5F)
                                             .build()));

        String optimisedSearch = handler.optimiseDefaultSearch("my query");
        assertThat(optimisedSearch, endsWith("+((" + expression + ")^4.5)"));
    }

    @Test
    void appendMultipleBoostExpression() {
        String expr1 = "myExpression:true";
        String expr2 = "myOtherExpression:[6 TO *]";
        DefaultSearchHandler handler = new DefaultSearchHandler(
                FakeSearchField.content,
                FakeSearchField.accession,
                FakeSearchField.getBoostFields(),
                () -> asList(BoostExpression.builder()
                                     .expression(expr1)
                                     .boost(4.5F)
                                     .build(),
                             BoostExpression.builder()
                                     .expression(expr2)
                                     .boost(10F)
                                     .build()));

        String optimisedSearch = handler.optimiseDefaultSearch("my query");
        assertThat(optimisedSearch, endsWith("+((" + expr1 + ")^4.5 (" + expr2 + ")^10.0)"));
    }

    private enum FakeSearchField implements SearchField {
        content(SearchFieldType.TERM, null, null),
        accession(SearchFieldType.TERM, FieldValueValidator::isAccessionValid, null),
        taxonomy_name(SearchFieldType.TERM, null, 2.0f),
        taxonomy_id(SearchFieldType.TERM, FieldValueValidator::isNumberValue, 2.0f),
        gene(SearchFieldType.TERM, null, 2.0f);

        private final Predicate<String> fieldValueValidator;
        private final SearchFieldType searchFieldType;
        private final Float boostValue;

        FakeSearchField(SearchFieldType searchFieldType, Predicate<String> fieldValueValidator, Float boostValue) {
            this.searchFieldType = searchFieldType;
            this.fieldValueValidator = fieldValueValidator;
            this.boostValue = boostValue;
        }

        private static List<SearchField> getBoostFields() {
            return asList(FakeSearchField.taxonomy_name, FakeSearchField.taxonomy_id, FakeSearchField.gene);
        }

        @Override
        public boolean hasValidValue(String value) {
            return this.fieldValueValidator == null || this.fieldValueValidator.test(value);
        }

        @Override
        public String getName() {
            return this.name();
        }

        @Override
        public Float getBoostValue() {
            return this.boostValue;
        }

        @Override
        public boolean hasBoostValue() {
            return boostValue != null;
        }
    }
}