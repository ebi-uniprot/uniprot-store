package uk.ac.ebi.uniprot.search.field;

import uk.ac.ebi.uniprot.search.field.validator.FieldValueValidator;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

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

    enum ResultFields {
        id("PubMed ID"),
        doi("Doi"),
        title("Title"),
        authoring_group("Authoring Group"),
        author("Authors"),
        author_and_group("Authors/Groups"),
        journal("Journal"),
        publication("Publication"),
        reference("Reference"),
        lit_abstract("Abstract/Summary"),
        statistics("Statistics");

        private String label;

        ResultFields(String label) {
            this.label = label;
        }

        public String getLabel() {
            return this.label;
        }
    }

    enum Return {
        id, literature_obj
    }
}
