package org.uniprot.store.search.field;

import java.util.function.Predicate;

/** @author lgonzales */
public interface SearchField {

    BoostValue getBoostValue();

    default boolean hasBoostValue() {
        return getBoostValue() != null;
    }

    default boolean hasValidValue(String value) {
        return getFieldValueValidator() == null || getFieldValueValidator().test(value);
    }

    String getName();

    SearchFieldType getSearchFieldType();

    Predicate<String> getFieldValueValidator();
}
