package org.uniprot.store.config.searchfield.schema;

import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.uniprot.store.config.schema.AbstractFieldValidator;
import org.uniprot.store.config.schema.SchemaValidationException;
import org.uniprot.store.config.searchfield.model.SearchFieldItem;

public class SearchFieldDataValidator extends AbstractFieldValidator<SearchFieldItem> {
    @Override
    public void validateContent(List<SearchFieldItem> fieldItems) {
        Set<String> ids = extractIds(fieldItems);
        validateParentExists(fieldItems, ids);
        validateSeqNumbers(fieldItems);
        validateChildNumbers(fieldItems);
        validateSortFieldIds(fieldItems, ids);
    }

    public void validateSortFieldIds(List<SearchFieldItem> fieldItems, Set<String> ids) {
        fieldItems.stream()
                .filter(SearchFieldDataValidator::hasSortFieldId)
                .forEach(
                        fi -> {
                            if (!ids.contains(fi.getSortFieldId())) {
                                throw new SchemaValidationException(
                                        "No field item with id for sortId " + fi.getSortFieldId());
                            }
                        });
    }

    private static boolean hasSortFieldId(SearchFieldItem fi) {
        return Objects.nonNull(fi.getSortFieldId());
    }
}
