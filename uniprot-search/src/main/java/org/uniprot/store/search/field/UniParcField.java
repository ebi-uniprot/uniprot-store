package org.uniprot.store.search.field;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.uniprot.store.search.field.validator.FieldValueValidator;

/**
 *
 * @author jluo
 * @date: 18 Jun 2019
 *
*/

public interface UniParcField {
	enum Return {
		upi, entry_stored;
	}
	
	 enum Sort {
		upi("upi"),
		length("length");

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
		upi(SearchFieldType.TERM, FieldValueValidator::isUpiValid, null), // uniparc upid
		accession(SearchFieldType.TERM, FieldValueValidator::isAccessionValid, null), // uniprot entry accession
		isoform(SearchFieldType.TERM, FieldValueValidator::isAccessionValid, null), // uniprot entry accession
		upid(SearchFieldType.TERM, FieldValueValidator::isUpidValid, null), // proteome upid
		
		taxonomy_id(SearchFieldType.TERM, FieldValueValidator::isNumberValue, null),
		taxonomy_name(SearchFieldType.TERM, null,null),
	 
		gene(SearchFieldType.TERM), 
		protein(SearchFieldType.TERM), 
		database(SearchFieldType.TERM), 
		active(SearchFieldType.TERM), 
		checksum(SearchFieldType.TERM), 
		length(SearchFieldType.TERM, FieldValueValidator::isNumberValue, null),
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

		public static List<SearchField> getBoostFields() {
			return Arrays.stream(Search.values())
					.filter(Search::hasBoostValue)
					.collect(Collectors.toList());
		}
	}
	enum ResultFields implements ReturnField{
		uniParcId,
		databaseCrossReferences,
		sequence,
		uniprotExclusionReason,
		sequenceFeatures,
		taxonomies;

		@Override
		public boolean hasReturnField(String fieldName) {
			return false;
		}

		@Override
		public String getJavaFieldName() {
			return this.name();
		}

		@Override
		public boolean isMandatoryJsonField() {
			return false;
		}
	}
}

