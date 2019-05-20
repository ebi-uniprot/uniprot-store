package uk.ac.ebi.uniprot.search.field;

import uk.ac.ebi.uniprot.search.field.validator.FieldValueValidator;

import java.util.function.Predicate;

public interface ProteomeField {
	 public enum Return {
	        upid,
	        proteome_stored;
	    };
	 
	    public enum Sort{
	    	upid("upid"),
	    	 proteome_type("proteome_type"),
	    	 annotation_score("annotation_score"),
	        organism("organism_sort");

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
	public static enum Search implements SearchField {
		upid(SearchFieldType.TERM,FieldValueValidator::isUpidValid, null),            // proteome upid
	    reference(SearchFieldType.TERM,FieldValueValidator::isBooleanValue, null),             // reference or not reference
	    redundant(SearchFieldType.TERM,FieldValueValidator::isBooleanValue, null),             // redundant or not redudant
		 annotation_score(SearchFieldType.TERM),
		 proteome_type(SearchFieldType.TERM),
		organism_name(SearchFieldType.TERM, null, 2.0f),
		organism_id(SearchFieldType.TERM, FieldValueValidator::isNumberValue, 2.0f),
		taxonomy_name(SearchFieldType.TERM, null,null),
		taxonomy_id(SearchFieldType.TERM, FieldValueValidator::isNumberValue, null),
		superkingdom(SearchFieldType.TERM),
		genome_accession(SearchFieldType.TERM),
		genome_assembly(SearchFieldType.TERM),
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

		public Predicate<String> getFieldValueValidator() {
			return this.fieldValueValidator;
		}

		public SearchFieldType getSearchFieldType() {
			return this.searchFieldType;
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

		@Override
		public String getName() {
			return this.name();
		}

	}

}
