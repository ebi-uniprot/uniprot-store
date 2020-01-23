package org.uniprot.store.search.field;

import java.util.Set;

import org.uniprot.store.search.domain2.SearchField;

/**
 * Represents a container of {@link org.uniprot.store.search.domain2.SearchField} instances,
 * providing utility methods to ease their access.
 *
 * <p>Created 14/11/19
 *
 * @author Edd
 */
public interface SearchFields {
    default boolean hasField(String field) {
        return getSearchFields().stream()
                .map(org.uniprot.store.search.domain2.SearchField::getName)
                .anyMatch(searchField -> searchField.equals(field));
    }

    default boolean hasSortField(String field) {
        return getSearchFields().stream()
                .filter(searchField -> searchField.getSortField().isPresent())
                .map(org.uniprot.store.search.domain2.SearchField::getName)
                .anyMatch(searchField -> searchField.equals(field));
    }

    default org.uniprot.store.search.domain2.SearchField getField(String field) {
        return getSearchFields().stream()
                .filter(searchField -> searchField.getName().equals(field))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown field: " + field));
    }

    default org.uniprot.store.search.domain2.SearchField getSortFieldFor(String field) {
        IllegalArgumentException exception =
                new IllegalArgumentException(
                        "Field '" + field + "' does not have an associated sort field.");
        for (org.uniprot.store.search.domain2.SearchField searchField : getSearchFields()) {
            if (searchField.getName().equals(field) && searchField.getSortField().isPresent()) {
                return searchField.getSortField().orElseThrow(() -> exception);
            }
        }
        throw exception;
    }

    default boolean fieldValueIsValid(String field, String value) {
        for (org.uniprot.store.search.domain2.SearchField searchField : getSearchFields()) {
            if (searchField.getName().equals(field)) {
                return searchField.getValidRegex().map(value::matches).orElse(true);
            }
        }
        throw new IllegalArgumentException("Field does not exist: " + field);
    }

    Set<org.uniprot.store.search.domain2.SearchField> getSearchFields();

    Set<SearchField> getSortFields();
}
