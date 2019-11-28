package org.uniprot.store.search.domain2;

import java.util.*;
import java.util.stream.Collectors;

/**
 * A validator for {@link SearchField}s.
 *
 * <p>Created 20/11/2019
 *
 * @author Edd
 */
class SearchFieldsValidator {
    static void validate(Collection<SearchField> searchFields) {
        searchFields.forEach(SearchFieldsValidator::checkMandatoryFields);
        verifyNoDuplicateFields(searchFields);
    }

    private static void checkMandatoryFields(SearchField field) {
        if (field.getName() == null || field.getType() == null) {
            throw new IllegalStateException(
                    "Mandatory search field value (name/type) missing for: " + field.getName());
        }
    }

    private static void verifyNoDuplicateFields(Collection<SearchField> searchFields) {
        List<String> fieldNames = new ArrayList<>();
        for (SearchField searchField : searchFields) {
            if (searchField.getType().equals(SearchFieldType.GENERAL)
                    || (searchField.getType().equals(SearchFieldType.RANGE)
                            && fieldNames.contains(searchField.getName()))) {
                fieldNames.add(searchField.getName());
            }
        }

        Set<String> allItems = new HashSet<>();
        Set<String> duplicates =
                fieldNames.stream().filter(name -> !allItems.add(name)).collect(Collectors.toSet());
        if (!duplicates.isEmpty()) {
            throw new IllegalStateException(
                    "Invalid duplicate field names found: "
                            + Arrays.toString(duplicates.toArray()));
        }
    }
}
