package org.uniprot.store.search.field;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;


import org.uniprot.store.search.field.validator.FieldValueValidator;

/**
 *
 * @author jluo
 * @date: 19 Aug 2019
 *
*/

public interface UniRefField {
	enum Search  implements SearchField {
		
		id(SearchFieldType.TERM, FieldValueValidator::isUniRefIdValid, null), // uniparc upid
		name(SearchFieldType.TERM),
		identity(SearchFieldType.TERM),
		count(SearchFieldType.RANGE),
		length(SearchFieldType.RANGE),
		created(SearchFieldType.RANGE),
		uniprotid(SearchFieldType.TERM),
		upi(SearchFieldType.TERM, FieldValueValidator::isUpiValid, null), // uniparc upid
		taxonomy_id(SearchFieldType.TERM, FieldValueValidator::isNumberValue, null),
		taxonomy_name(SearchFieldType.TERM, null,null),
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
}

