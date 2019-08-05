package org.uniprot.store.search;

import org.junit.jupiter.api.Test;
import org.uniprot.store.search.DefaultSearchHandler;
import org.uniprot.store.search.field.BoostValue;
import org.uniprot.store.search.field.SearchField;
import org.uniprot.store.search.field.SearchFieldType;
import org.uniprot.store.search.field.validator.FieldValueValidator;

import java.util.List;
import java.util.function.Predicate;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.store.search.field.BoostValue.boostValue;

class DefaultSearchHandlerTest {
    private final DefaultSearchHandler defaultSearchHandler =
            new DefaultSearchHandler(FakeSearchField.content,
                                     FakeSearchField.accession,
                                     FakeSearchField.getBoostFields());

    private final DefaultSearchHandler singleValueBoostSearchHandler =
            new DefaultSearchHandler(FakeSearchField.content,
                                     FakeSearchField.accession,
                                     FakeSearchField.getSingleValueBoostFields());

    private final DefaultSearchHandler multiValueBoostSearchHandler =
            new DefaultSearchHandler(FakeSearchField.content,
                                     FakeSearchField.accession,
                                     FakeSearchField.getMultipleBoostFields());

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
    void rewriteDefaultSearchPhraseQuery() {
        String defaultQuery = "\"Homo Sapiens\"";
        String result = defaultSearchHandler.optimiseDefaultSearch(defaultQuery);
        assertNotNull(result);
        String expectedResult = "content:\"Homo Sapiens\" (taxonomy_name:\"Homo Sapiens\")^2.0 (gene:\"Homo Sapiens\")^2.0";
        assertEquals(expectedResult, result);
    }

    @Test
    void rewriteDefaultSearchPhraseQueryAndOthersTerms() {
        String defaultQuery = "organism:9606 \"homo sapiens\"";
        String result = defaultSearchHandler.optimiseDefaultSearch(defaultQuery);
        assertNotNull(result);
        String expectedResult = "+organism:9606 " +
                "+(content:\"homo sapiens\" (taxonomy_name:\"homo sapiens\")^2.0 (gene:\"homo sapiens\")^2.0)";
        assertEquals(expectedResult, result);
    }

    @Test
    void convertSingleValueBoostField() {
        String defaultQuery = "human";
        String result = singleValueBoostSearchHandler.optimiseDefaultSearch(defaultQuery);
        assertNotNull(result);
        String expectedResult = "content:human (reviewed:true)^8.0";
        assertEquals(expectedResult, result);
    }

    @Test
    void convertMultiValueBoostField() {
        String defaultQuery = "human";
        String result = multiValueBoostSearchHandler.optimiseDefaultSearch(defaultQuery);
        assertNotNull(result);
        String expectedResult = "content:human (name:\"two words\")^10.0";
        assertEquals(expectedResult, result);
    }

    private enum FakeSearchField implements SearchField {
        content(SearchFieldType.TERM, null, null),
        accession(SearchFieldType.TERM, FieldValueValidator::isAccessionValid, null),
        taxonomy_name(SearchFieldType.TERM, null, boostValue(2.0f)),
        taxonomy_id(SearchFieldType.TERM, FieldValueValidator::isNumberValue, boostValue(2.0f)),
        reviewed(SearchFieldType.TERM, FieldValueValidator::isBooleanValue, boostValue("true", 8.0f)),
        name(SearchFieldType.TERM, null, boostValue("two words", 10.0f)),
        gene(SearchFieldType.TERM, null, boostValue(2.0f));

        private final Predicate<String> fieldValueValidator;
        private final SearchFieldType searchFieldType;
        private final BoostValue boostValue;

        FakeSearchField(SearchFieldType searchFieldType, Predicate<String> fieldValueValidator, BoostValue boostValue) {
            this.searchFieldType = searchFieldType;
            this.fieldValueValidator = fieldValueValidator;
            this.boostValue = boostValue;
        }

        private static List<SearchField> getBoostFields() {
            return asList(FakeSearchField.taxonomy_name, FakeSearchField.taxonomy_id,
                          FakeSearchField.gene);
        }

        private static List<SearchField> getSingleValueBoostFields() {
            return singletonList(FakeSearchField.reviewed);
        }

        private static List<SearchField> getMultipleBoostFields() {
            return singletonList(FakeSearchField.name);
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
        public SearchFieldType getSearchFieldType() {
            return searchFieldType;
        }

        @Override
        public Predicate<String> getFieldValueValidator() {
            return fieldValueValidator;
        }

        @Override
        public BoostValue getBoostValue() {
            return this.boostValue;
        }

        @Override
        public boolean hasBoostValue() {
            return boostValue != null;
        }
    }
}