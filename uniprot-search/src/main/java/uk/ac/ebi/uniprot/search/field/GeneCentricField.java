package uk.ac.ebi.uniprot.search.field;

import uk.ac.ebi.uniprot.search.field.validator.FieldValueValidator;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * @author jluo
 * @date: 17 May 2019
 */

public interface GeneCentricField {
    public enum Return {
        accession_id, genecentric_stored;
    }

    ;

    enum ResultFields implements ReturnField {
        accession_id("canonical protein"),
        gene("canonical gene"),
        entry_type("entry type"),
        related_accession("related proteins");

        private String label;

        private ResultFields(String label) {
            this.label = label;
        }


        @Override
        public boolean hasReturnField(String fieldName) {
            return Arrays.stream(ResultFields.values())
                    .anyMatch(returnItem -> returnItem.name().equalsIgnoreCase(fieldName));
        }
    }

    ;

    public enum Sort {
        accession_id("accession_id");

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

    public static enum Search implements SearchField {
        accession_id(SearchFieldType.TERM, FieldValueValidator::isAccessionValid, null), // uniprot entry accession
        accession(SearchFieldType.TERM, FieldValueValidator::isAccessionValid, null), // uniprot entry accession
        upid(SearchFieldType.TERM, FieldValueValidator::isUpidValid, null), // proteome upid
        organism_id(SearchFieldType.TERM, FieldValueValidator::isNumberValue, 2.0f),
        gene(SearchFieldType.TERM),
        reviewed(SearchFieldType.TERM, FieldValueValidator::isBooleanValue, null),
        content(SearchFieldType.TERM); //used in the default search


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
        public Predicate<String> getFieldValueValidator() {
            return this.fieldValueValidator;
        }

        @Override
        public SearchFieldType getSearchFieldType() {
            return this.searchFieldType;
        }

        @Override
        public Float getBoostValue() {
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

}
