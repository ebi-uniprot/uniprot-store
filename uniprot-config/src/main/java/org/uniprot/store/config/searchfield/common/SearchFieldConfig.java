package org.uniprot.store.config.searchfield.common;

import java.util.List;

import org.uniprot.store.config.searchfield.model.FieldItem;
import org.uniprot.store.config.searchfield.model.FieldType;

public interface SearchFieldConfig {
    // common methods
    List<FieldItem> getAllFieldItems();

    FieldType getFieldTypeBySearchFieldName(String fieldName);

    // Search fields related methods
    List<FieldItem> getSearchFieldItems();

    FieldItem getSearchFieldItemByName(String fieldName);

    Boolean isSearchFieldValueValid(String fieldName, String value);

    Boolean doesSearchFieldItemExist(String fieldName);

    // sort related methods
    FieldItem getCorrespondingSortField(String searchFieldName);

    Boolean doesCorrespondingSortFieldExist(String searchFieldName);

    List<FieldItem> getSortFieldItems();
}
