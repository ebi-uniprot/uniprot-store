package org.uniprot.store.search.field;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.uniprot.store.search.field.validator.FieldValueValidator;

public interface CrossRefField {

    enum Sort {
        accession("accession"),
        category_str("category_str");

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
        accession(SearchFieldType.TERM, FieldValueValidator::isCrossRefIdValue, null),
        name(SearchFieldType.TERM),
        category_facet(SearchFieldType.TERM),
        content(SearchFieldType.TERM);

        private final Predicate<String> fieldValueValidator;
        private final SearchFieldType searchFieldType;
        private final BoostValue boostValue;

        Search(SearchFieldType searchFieldType, Predicate<String> fieldValueValidator, BoostValue boostValue) {
            this.searchFieldType = searchFieldType;
            this.fieldValueValidator = fieldValueValidator;
            this.boostValue = boostValue;
        }

        Search(SearchFieldType searchFieldType) {
            this.searchFieldType = searchFieldType;
            this.fieldValueValidator = null;
            this.boostValue = null;
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
        name("Name", "name", true),
        accession("Accession", "accession", true),
        abbrev("Abbrev", "abbrev", true),
        pub_med_id("Pub Med Id", "pubMedId", true),
        doi_id("DOI Id", "doiId", true),
        link_type("Link Type", "linkType", true),
        server("Server", "server", true),
        db_url("DB URL", "dbUrl", true),
        category("Category", "category", true),
        reviewed_protein_count("Reviewed Protein Count", "reviewedProteinCount", true),
        unreviewed_protein_count("Unreviewed Protein Count", "unreviewedProteinCount", true);

        private String label;
        private String fieldName;
        private boolean isDefault;

        ResultFields(String label, String fieldName, boolean isDefault) {
            this.label = label;
            this.fieldName = fieldName;
            this.isDefault = isDefault;
        }

        public String getLabel() {
            return this.label;
        }

        public String getFieldName() {
            return this.fieldName;
        }

        public boolean isDefault() {
            return this.isDefault;
        }

        @Override
        public boolean hasReturnField(String fieldName) {
            return Arrays.stream(ResultFields.values())
                    .anyMatch(returnItem -> returnItem.name().equalsIgnoreCase(fieldName));
        }

        @Override
        public String getJavaFieldName() {
            return this.fieldName;
        }
    }
}
