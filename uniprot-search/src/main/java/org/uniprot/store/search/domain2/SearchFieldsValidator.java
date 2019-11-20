package org.uniprot.store.search.domain2;

import org.uniprot.core.util.Utils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created 20/11/2019
 *
 * @author Edd
 */
class SearchFieldsValidator {
    static void validate(Collection<SearchField> searchFields) {
        verifyNoDuplicateFields(searchFields);

        searchFields.forEach(
                searchField -> {
                    fieldsMustHaveType(searchField);
                    sortFieldMustHaveField(searchField);
                });
    }

    private static void fieldsMustHaveType(SearchField field) {
        if (field.getType() == null) {
            throw new IllegalStateException(
                    "Mandatory search field type missing for: " + field.getName());
        }
    }

    private static void sortFieldMustHaveField(SearchField field) {
        field.getSortName().ifPresent(sortName -> {
            if(Utils.nullOrEmpty(field.getName())){
                throw new IllegalStateException(
                    "Sort field ("
                        + sortName
                        + ") must have an associated field name.");
            }
        });
    }

    private static void verifyNoDuplicateFields(Collection<SearchField> searchFields) {
        List<String> fieldNames = new ArrayList<>();
        for (SearchField searchField : searchFields) {
            fieldNames.add(searchField.getName());
            searchField
                    .getSortName()
                    .filter(sortName -> !sortName.equals(searchField.getName()))
                    .ifPresent(fieldNames::add);
        }

        Set<String> allItems = new HashSet<>();
        Set<String> duplicates =
                fieldNames.stream().filter(name -> !allItems.add(name)).collect(Collectors.toSet());
        if (!duplicates.isEmpty()) {
            throw new IllegalStateException(
                    "Duplicate field names found: " + Arrays.toString(duplicates.toArray()));
        }
    }
}
