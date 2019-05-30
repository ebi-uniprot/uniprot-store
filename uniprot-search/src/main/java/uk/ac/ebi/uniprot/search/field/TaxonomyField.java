package uk.ac.ebi.uniprot.search.field;

import uk.ac.ebi.uniprot.search.field.validator.FieldValueValidator;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

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
        linked(SearchFieldType.TERM,FieldValueValidator::isBooleanValue, null),
        active(SearchFieldType.TERM,FieldValueValidator::isBooleanValue, null),
        complete(SearchFieldType.TERM,FieldValueValidator::isBooleanValue, null),
        reference(SearchFieldType.TERM,FieldValueValidator::isBooleanValue, null),
        reviewed(SearchFieldType.TERM,FieldValueValidator::isBooleanValue, null),
        annotated(SearchFieldType.TERM,FieldValueValidator::isBooleanValue, null),
        content(SearchFieldType.TERM);

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
        public boolean hasBoostValue() {
            return boostValue != null;
        }

        @Override
        public boolean hasValidValue(String value) {
            return this.fieldValueValidator == null || this.fieldValueValidator.test(value);
        }

        @Override
        public String getName() {
            return this.name();
        }

        public static List<SearchField> getBoostFields(){
            return Arrays.stream(Search.values())
                    .filter(Search::hasBoostValue)
                    .collect(Collectors.toList());
        }
    }

    enum ResultFields{
        id("Taxon"),
        parent("Parent"),
        mnemonic("Mnemonic"),
        scientific_name("Scientific name"),
        common_name("Common name"),
        synonym("Synonym"),
        other_names("Other Names"),
        rank("Rank"),
        lineage("Lineage"),
        strain("Strain"),
        host("Virus hosts"),
        link("Link");

        private String label;

        private ResultFields(String label){
            this.label = label;
        }

        public String getLabel(){
            return this.label;
        }



    }

    enum Return {
        id,taxonomy_obj
    }
}
