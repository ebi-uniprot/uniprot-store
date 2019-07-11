package uk.ac.ebi.uniprot.search.field;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public interface DiseaseField {

    enum Sort{
        accession("accession");

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
        accession(SearchFieldType.TERM),
        name(SearchFieldType.TERM),
        content(SearchFieldType.TERM);

        private final Predicate<String> fieldValueValidator;
        private final SearchFieldType searchFieldType;
        private final BoostValue boostValue;

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

        public static List<SearchField> getBoostFields(){
            return Arrays.stream(Search.values())
                    .filter(Search::hasBoostValue)
                    .collect(Collectors.toList());
        }
    }

    enum ResultFields implements ReturnField {
        id("Name"),
        accession("Disease ID"),
        acronym("Mnemonic"),
        definition("Description"),
        alternative_names("Alternative Names"),
        cross_references("Cross Reference"),
        keywords("Keywords"),
        reviewed_protein_count("Reviewed Protein Count"),
        unreviewed_protein_count("Unreviewed Protein Count");

        private String label;

        ResultFields(String label){
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
}
