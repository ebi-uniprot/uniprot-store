package org.uniprot.store.search.domain2;

import java.util.Set;

/**
 * Created 14/11/19
 *
 * @author Edd
 */
public interface SearchFields {
    default boolean hasField(String field) {
        return getSearchFields().stream()
                .map(SearchField::getTerm)
                .anyMatch(searchField -> searchField.equals(field));
    }

    default boolean hasSortField(String field) {
        return getSearchFields().stream()
                .filter(searchField -> searchField.getSortTerm().isPresent())
                .map(SearchField::getTerm)
                .anyMatch(searchField -> searchField.equals(field));
    }

    default String getField(String field) {
        return getSearchFields().stream()
                .filter(searchField -> searchField.getTerm().equals(field))
                .map(SearchField::getTerm)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown field: " + field));
    }

    default String getSortFieldFor(String field) {
        IllegalArgumentException exception = new IllegalArgumentException(
                "Field '"
                        + field
                        + "' does not have an associated sort field.");
        for (SearchField searchField : getSearchFields()) {
            if (searchField.getTerm().equals(field) && searchField.getSortTerm().isPresent()) {
                return searchField
                        .getSortTerm()
                        .orElseThrow(() -> exception);
            }
        }
        throw exception;
    }

    default boolean fieldValueIsValid(String field, String value) {
        for (SearchField searchField : getSearchFields()) {
            if (searchField.getTerm().equals(field)) {
                return searchField.getValidRegex().map(value::matches).orElse(true);
            }
        }
        throw new IllegalArgumentException("Field does not exist: " + field);
    }

    Set<SearchField> getSearchFields();

    Set<SearchField> getGeneralFields();

    Set<SearchField> getRangeFields();

    Set<String> getSorts();
}
