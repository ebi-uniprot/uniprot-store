package uk.ac.ebi.uniprot.search.field;

import uk.ac.ebi.uniprot.search.field.validator.FieldValueValidator;

import java.util.function.Predicate;

public interface TaxonomyField {

    enum Sort{
        name("name_sort");

        private String solrFieldName;

        Sort(String solrFieldName){
            this.solrFieldName = solrFieldName;
        }

        public String getSolrFieldName() {
            return solrFieldName;
        }

        @Override
        public String toString() {
            return this.solrFieldName;
        }
    }

    enum Search implements SearchField {
        id(SearchFieldType.TERM, FieldValueValidator::isNumberValue, null),
        tax_id(SearchFieldType.TERM, FieldValueValidator::isNumberValue, null),
        scientific(SearchFieldType.TERM),
        common(SearchFieldType.TERM),
        mnemonic(SearchFieldType.TERM),
        rank(SearchFieldType.TERM),
        strain(SearchFieldType.TERM),
        host(SearchFieldType.TERM, FieldValueValidator::isNumberValue, null),
        linked(SearchFieldType.TERM,FieldValueValidator::isBooleanValue, null);

        private final Predicate<String> fieldValueValidator;
        private final SearchFieldType searchFieldType;
        private final Float boostValue;

        Search(SearchFieldType searchFieldType) {
            this.searchFieldType = searchFieldType;
            this.fieldValueValidator = null;
            this.boostValue = null;
        }

        Search(SearchFieldType searchFieldType, Predicate<String> fieldValueValidator, Float boostValue) {
            this.searchFieldType = searchFieldType;
            this.fieldValueValidator = fieldValueValidator;
            this.boostValue = boostValue;
        }

        public Predicate<String> getFieldValueValidator() {
            return this.fieldValueValidator;
        }

        public SearchFieldType getSearchFieldType() {
            return this.searchFieldType;
        }

        @Override
        public Float getBoostValue() {
            return this.boostValue;
        }

        @Override
        public boolean hasValidValue(String value) {
            return this.fieldValueValidator == null || this.fieldValueValidator.test(value);
        }

        @Override
        public String getName() {
            return this.name();
        }
    }

    enum Return {
        id,taxonomy_obj
    }
}
