package org.uniprot.store.search.field;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.uniprot.store.search.field.validator.FieldValueValidator;

/**
 * @author lgonzales
 * @since 2019-07-19
 */
public interface SubcellularLocationField {

    enum Sort {
        id("id"),
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
        id(SearchFieldType.TERM, FieldValueValidator::isSubcellularLocationIdValue, null),
        name(SearchFieldType.TERM),
        category(SearchFieldType.TERM),
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

        @Override
        public BoostValue getBoostValue() {
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

        public SearchFieldType getSearchFieldType() {
            return searchFieldType;
        }

        public Predicate<String> getFieldValueValidator() {
            return this.fieldValueValidator;
        }

        @Override
        public String getName() {
            return this.name();
        }

        public static List<SearchField> getBoostFields() {
            return Arrays.stream(SubcellularLocationField.Search.values())
                    .filter(SubcellularLocationField.Search::hasBoostValue)
                    .collect(Collectors.toList());
        }
    }

    enum ResultFields implements ReturnField {
        id("Alias", "id", true),
        accession("Subcellular location ID", "accession"),
        definition("Description", "definition"),
        category("Category", "category"),
        keyword("Keyword", "keyword"),
        synonyms("Synonyms", "synonyms"),
        content("Content", "content"),
        gene_ontologies("Gene Ontologies", "geneOntologies"),
        note("Note", "note"),
        references("References", "references"),
        links("Links", "links"),
        is_a("Is a", "isA"),
        part_of("Is part of", "partOf"),
        statistics("Statistics", "statistics");

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

    enum Return {
        id,
        subcellularlocation_obj
    }
}
