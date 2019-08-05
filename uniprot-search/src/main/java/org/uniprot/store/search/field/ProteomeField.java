package org.uniprot.store.search.field;

import static org.uniprot.store.search.field.BoostValue.boostValue;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.uniprot.store.search.field.validator.FieldValueValidator;

public interface ProteomeField {
    enum Return {
        upid,
        proteome_stored;
    }

    enum Sort {
        upid("upid"),
        proteome_type("proteome_type"),
        annotation_score("annotation_score"),
        organism_sort("organism_sort");

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
        upid(SearchFieldType.TERM, FieldValueValidator::isUpidValid, null),            // proteome upid
        reference(SearchFieldType.TERM, FieldValueValidator::isBooleanValue, null),             // reference or not reference
        redundant(SearchFieldType.TERM, FieldValueValidator::isBooleanValue, null),             // redundant or not redudant
        annotation_score(SearchFieldType.TERM),
        proteome_type(SearchFieldType.TERM),
        organism_name(SearchFieldType.TERM, null, boostValue(2.0f)),
        organism_id(SearchFieldType.TERM, FieldValueValidator::isNumberValue, boostValue(2.0f)),
        taxonomy_name(SearchFieldType.TERM, null, null),
        taxonomy_id(SearchFieldType.TERM, FieldValueValidator::isNumberValue, null),
        superkingdom(SearchFieldType.TERM),
        genome_accession(SearchFieldType.TERM),
        genome_assembly(SearchFieldType.TERM),
        content(SearchFieldType.TERM); //used in the default search

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
        public Predicate<String> getFieldValueValidator() {
            return this.fieldValueValidator;
        }

        @Override
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
        id,
        description,
        taxonomy,
        modified,
        proteomeType,
        redundantTo,
        strain,
        isolate,
        dbXReferences,
        components,
        references,
        redundantProteomes,
        panproteome,
        annotationScore,
        superkingdom,
        geneCount,
        taxonLineage,
        canonicalProteins,
        sourceDb;

        @Override
        public boolean hasReturnField(String fieldName) {
            return false;
        }

        @Override
        public String getJavaFieldName() {
            return this.name();
        }
    }
}
