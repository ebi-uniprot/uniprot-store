package org.uniprot.store.config.searchfield.schema;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.uniprot.store.config.schema.FieldDataValidator;
import org.uniprot.store.config.schema.SchemaValidationException;
import org.uniprot.store.config.searchfield.model.SearchFieldDataType;
import org.uniprot.store.config.searchfield.model.SearchFieldItem;
import org.uniprot.store.config.searchfield.model.SearchFieldItemType;

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
        validateSearchFieldDataType(fieldItems);
        validateSortFieldIds(fieldItems, extractIds(fieldItems));
    }

    private void validateSearchFieldDataType(List<SearchFieldItem> fieldItems) {
        fieldItems.stream()
                .filter(fi -> fi.getValues() != null && !fi.getValues().isEmpty())
                .filter(fi -> !SearchFieldDataType.ENUM.equals(fi.getDataType()))
                .findFirst()
                .ifPresent(
                        fi -> {
                            throw new SchemaValidationException(
                                    "Field item " + fi.getFieldName() + "should be an ENUM");
                        });
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
