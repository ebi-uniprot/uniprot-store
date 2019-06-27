package uk.ac.ebi.uniprot.search.field;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import uk.ac.ebi.uniprot.search.field.validator.FieldValueValidator;

public interface KeywordField {

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
        id(SearchFieldType.TERM, FieldValueValidator::isKeywordIdValue, null),
        keyword_id(SearchFieldType.TERM, FieldValueValidator::isKeywordIdValue, null),
        name(SearchFieldType.TERM),
        ancestor(SearchFieldType.TERM),
        parent(SearchFieldType.TERM),
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
            return Arrays.stream(Search.values())
                    .filter(Search::hasBoostValue)
                    .collect(Collectors.toList());
        }

    }

    public enum ResultFields implements ReturnField{
        id("Keyword ID"),
        name("Name"),
        description("Description"),
        category("Category"),
        synonym("Synonyms"),
        gene_ontology("Gene Ontology"),
        sites("Sites"),
        children("Children"),
        parent("Parents"),
        statistics("Statistics");

        private String label;

        private ResultFields(String label) {
            this.label = label;
        }

        public String getLabel() {
            return this.label;
        }

		@Override
		public boolean hasReturnField(String fieldName) {
			 return Arrays.stream(ResultFields.values())
	                    .anyMatch(returnItem -> returnItem.name().equalsIgnoreCase(fieldName));
		}
    }

    enum Return {
        id, taxonomy_obj
    }
}
