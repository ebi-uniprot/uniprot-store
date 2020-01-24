package org.uniprot.store.search.field;

import org.uniprot.store.search.domain2.SearchField;

import java.util.Set;

/**
 * Represents a container of {@link SearchField} instances,
 * providing utility methods to ease their access.
 *
 * <p>Created 14/11/19
 *
 * @author Edd
 */
public interface SearchFields {
    default boolean hasField(String field) {
        return getSearchFields().stream()
                .map(SearchField::getName)
                .anyMatch(searchField -> searchField.equals(field));
    }

    default boolean hasSortField(String field) {
        return getSearchFields().stream()
                .filter(searchField -> searchField.getSortField().isPresent())
                .map(SearchField::getName)
                .anyMatch(searchField -> searchField.equals(field));
    }

    default SearchField getField(String field) {
        return getSearchFields().stream()
                .filter(searchField -> searchField.getName().equals(field))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown field: " + field));
    }

    default SearchField getSortFieldFor(String field) {
        IllegalArgumentException exception =
                new IllegalArgumentException(
                        "Field '" + field + "' does not have an associated sort field.");
        for (SearchField searchField : getSearchFields()) {
            if (searchField.getName().equals(field) && searchField.getSortField().isPresent()) {
                return searchField.getSortField().orElseThrow(() -> exception);
            }
        }
        throw exception;
    }

    default boolean fieldValueIsValid(String field, String value) {
        for (SearchField searchField : getSearchFields()) {
            if (searchField.getName().equals(field)) {
                return searchField.getValidRegex().map(value::matches).orElse(true);
            }
        }
        throw new IllegalArgumentException("Field does not exist: " + field);
    }

    Set<SearchField> getSearchFields();

    Set<SearchField> getSortFields();
}
