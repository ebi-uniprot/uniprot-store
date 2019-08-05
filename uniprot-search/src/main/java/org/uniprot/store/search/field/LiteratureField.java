package org.uniprot.store.search.field;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.uniprot.store.search.field.validator.FieldValueValidator;

/**
 * @author lgonzales
 */
public interface LiteratureField {

    enum Sort {
        id("id_sort"),
        title("title_sort");

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
        doi(SearchFieldType.TERM),
        title(SearchFieldType.TERM),
        author(SearchFieldType.TERM),
        journal(SearchFieldType.TERM),
        published(SearchFieldType.TERM),
        citedin(SearchFieldType.TERM, FieldValueValidator::isBooleanValue, null),
        mappedin(SearchFieldType.TERM, FieldValueValidator::isBooleanValue, null),
        content(SearchFieldType.TERM);

        private final Predicate<String> fieldValueValidator;
        private final SearchFieldType searchFieldType;
        private final BoostValue boostValue;

        Search(SearchFieldType searchFieldType) {
            this.searchFieldType = searchFieldType;
            this.fieldValueValidator = null;
            this.boostValue = null;
        }

        Search(SearchFieldType searchFieldType, Predicate<String> fieldValueValidator, BoostValue boostValue) {
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
            return Arrays.stream(LiteratureField.Search.values())
                    .filter(LiteratureField.Search::hasBoostValue)
                    .collect(Collectors.toList());
        }

    }

    enum ResultFields implements ReturnField {
        id("PubMed ID", "pubmedId", true),
        doi("Doi", "doiId"),
        title("Title", "title"),
        authoring_group("Authoring Group", "authoringGroup"),
        author("Authors", "authors"),
        author_and_group("Authors/Groups"),
        journal("Journal", "journal"),
        publication("Publication", "publicationDate"),
        reference("Reference"),
        lit_abstract("Abstract/Summary", "literatureAbstract"),
        mapped_references("Mapped references", "literatureMappedReferences"),
        statistics("Statistics", "statistics"),
        first_page("First page", "firstPage"),
        last_page("Last page", "lastPage"),
        volume("Volume", "volume"),
        completeAuthorList("Complete Author List", "completeAuthorList", true);

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
        id, literature_obj
    }
}
