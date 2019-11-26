package org.uniprot.store.search.field;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.uniprot.store.search.field.validator.FieldValueValidator;

public interface DiseaseField {

    enum Sort {
        accession("accession");

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
        accession(SearchFieldType.TERM, FieldValueValidator::isDiseaseIdValue, null),
        name(SearchFieldType.TERM),
        content(SearchFieldType.TERM);

        private final Predicate<String> fieldValueValidator;
        private final SearchFieldType searchFieldType;
        private final BoostValue boostValue;

        Search(
                SearchFieldType searchFieldType,
                Predicate<String> fieldValueValidator,
                BoostValue boostValue) {
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
        id("Name", "id", true),
        accession("Disease ID", "accession", true),
        acronym("Mnemonic", "acronym", true),
        definition("Description", "definition", true),
        alternative_names("Alternative Names", "alternativeNames"),
        cross_references("Cross Reference", "crossReferences"),
        keywords("Keywords", "keywords"),
        reviewed_protein_count("Reviewed Protein Count", "reviewedProteinCount"),
        unreviewed_protein_count("Unreviewed Protein Count", "unreviewedProteinCount");

        private String label;
        private String javaFieldName;
        private boolean isDefault;

        ResultFields(String label, String javaFieldName) {
            this(label, javaFieldName, false);
        }

        ResultFields(String label, String javaFieldName, boolean isDefault) {
            this.label = label;
            this.javaFieldName = javaFieldName;
            this.isDefault = isDefault;
        }

        public String getLabel() {
            return this.label;
        }

        public boolean isDefault() {
            return this.isDefault;
        }

        public static String getDefaultFields() {
            return Arrays.stream(ResultFields.values())
                    .filter(f -> f.isDefault)
                    .map(f -> f.name())
                    .collect(Collectors.joining(","));
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
    }
}
