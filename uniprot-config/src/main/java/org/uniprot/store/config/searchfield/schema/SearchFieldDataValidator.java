package org.uniprot.store.config.searchfield.schema;

import org.uniprot.store.config.schema.FieldDataValidator;
import org.uniprot.store.config.schema.SchemaValidationException;
import org.uniprot.store.config.searchfield.model.SearchFieldItem;
import org.uniprot.store.config.searchfield.model.SearchFieldItemType;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class SearchFieldDataValidator extends FieldDataValidator<SearchFieldItem> {
    @Override
    protected List<SearchFieldItem> extractParentNodes(List<SearchFieldItem> fieldItems) {
        return fieldItems.stream()
                .filter(
                        field ->
                                field.getItemType() != null
                                        && (field.getItemType().equals(SearchFieldItemType.GROUP)
                                                || field.getItemType()
                                                        .equals(SearchFieldItemType.SIBLING_GROUP)))
                .collect(Collectors.toList());
    }

    @Override
    public void validateContent(List<SearchFieldItem> fieldItems) {
        super.validateContent(fieldItems);
        validateSortFieldIds(fieldItems, extractIds(fieldItems));
    }

    private void validateSortFieldIds(List<SearchFieldItem> fieldItems, Set<String> ids) {
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
