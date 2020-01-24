package org.uniprot.store.search.field;

import java.util.function.Predicate;

/** @author lgonzales */
public interface SearchField {

    default BoostValue getBoostValue() {
        return null;
    }

    default boolean hasBoostValue() {
        return getBoostValue() != null;
    }

    default boolean hasValidValue(String value) {
        return getFieldValueValidator() == null || getFieldValueValidator().test(value);
    }

    default String getName() {
        return null;
    }

    default SearchFieldType getSearchFieldType() {
        return null;
    }

    default Predicate<String> getFieldValueValidator() {
        return null;
    }
}
