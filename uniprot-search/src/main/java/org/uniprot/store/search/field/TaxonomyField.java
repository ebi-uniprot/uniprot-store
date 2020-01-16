package org.uniprot.store.search.field;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.uniprot.store.search.field.validator.FieldValueValidator;

public interface TaxonomyField {

    enum Sort {
        name("name_sort");

        private String solrFieldName;

        Sort(String solrFieldName) {
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
        linked(SearchFieldType.TERM, FieldValueValidator::isBooleanValue, null),
        active(SearchFieldType.TERM, FieldValueValidator::isBooleanValue, null),
        proteome(SearchFieldType.TERM, FieldValueValidator::isBooleanValue, null),
        reference(SearchFieldType.TERM, FieldValueValidator::isBooleanValue, null),
        reviewed(SearchFieldType.TERM, FieldValueValidator::isBooleanValue, null),
        annotated(SearchFieldType.TERM, FieldValueValidator::isBooleanValue, null),
        content(SearchFieldType.TERM);

        private final Predicate<String> fieldValueValidator;
        private final SearchFieldType searchFieldType;
        private final BoostValue boostValue;

        Search(SearchFieldType searchFieldType) {
            this.searchFieldType = searchFieldType;
            this.fieldValueValidator = null;
            this.boostValue = null;
        }

        Search(
                SearchFieldType searchFieldType,
                Predicate<String> fieldValueValidator,
                BoostValue boostValue) {
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
        public BoostValue getBoostValue() {
            return this.boostValue;
        }

        @Override
        public String getName() {
            return this.name();
        }

        public static List<SearchField> getBoostFields() {
            return Arrays.stream(Search.values())
                    .filter(Search::hasBoostValue)
                    .collect(Collectors.toList());
        }
    }

    enum ResultFields implements ReturnField {
        id("Taxon"),
        taxonId("Taxon Id", "taxonId", true), // always present in json response
        parent("Parent", "parentId"),
        mnemonic("Mnemonic", "mnemonic"),
        scientific_name("Scientific name", "scientificName"),
        common_name("Common name", "commonName"),
        synonym("Synonym", "synonyms"),
        other_names("Other Names", "otherNames"),
        rank("Rank", "rank"),
        reviewed("Reviewed", "reviewed"),
        lineage("Lineage", "lineage"),
        strain("Strain", "strains"),
        host("Virus hosts", "hosts"),
        link("Link", "links"),
        statistics("Statistics", "statistics"),
        active_internal("Active", "active"),
        inactiveReason_internal("Inactive Reason", "inactiveReason");

        private String label;
        private String javaFieldName;
        private boolean isMandatoryJsonField;

        ResultFields(String label) {
            this(label, null);
        }

        ResultFields(String label, String javaFieldName) {
            this(label, javaFieldName, false);
        }

        ResultFields(String label, String javaFieldName, boolean isMandatoryJsonField) {
            this.label = label;
            this.javaFieldName = javaFieldName;
            this.isMandatoryJsonField = isMandatoryJsonField;
        }

        public String getLabel() {
            return this.label;
        }

        @Override
        public boolean hasReturnField(String fieldName) {
            return Arrays.stream(ResultFields.values())
                    .anyMatch(returnItem -> returnItem.name().equalsIgnoreCase(fieldName));
        }

        @Override
        public String getJavaFieldName() {
            return this.javaFieldName;
        }

        @Override
        public boolean isMandatoryJsonField() {
            return this.isMandatoryJsonField;
        }
    }
}
