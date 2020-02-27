package org.uniprot.store.config.searchfield.common;

import java.util.List;

import org.uniprot.store.config.searchfield.model.SearchFieldItem;
import org.uniprot.store.config.searchfield.model.SearchFieldType;

public interface SearchFieldConfig {
    // common methods
    List<SearchFieldItem> getAllFieldItems();

    SearchFieldType getFieldTypeBySearchFieldName(String fieldName);

    // Search fields related methods
    List<SearchFieldItem> getSearchFieldItems();

    SearchFieldItem getSearchFieldItemByName(String fieldName);

    Boolean isSearchFieldValueValid(String fieldName, String value);

    Boolean doesSearchFieldItemExist(String fieldName);

    // sort related methods
    SearchFieldItem getCorrespondingSortField(String searchFieldName);

    Boolean doesCorrespondingSortFieldExist(String searchFieldName);

    List<SearchFieldItem> getSortFieldItems();
}
