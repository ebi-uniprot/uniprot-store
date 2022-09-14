package org.uniprot.store.config.searchfield.common;

import java.util.List;
import java.util.Set;

import org.uniprot.store.config.searchfield.model.SearchFieldItem;
import org.uniprot.store.config.searchfield.model.SearchFieldType;

public interface SearchFieldConfig {
    // common methods
    List<SearchFieldItem> getAllFieldItems();

    SearchFieldType getFieldTypeBySearchFieldName(String fieldName);

    // Search fields related methods
    List<SearchFieldItem> getSearchFieldItems();

    Set<String> getSearchFieldNames();

    SearchFieldItem getSearchFieldItemByName(String fieldName);

    boolean isSearchFieldValueValid(String fieldName, String value);

    boolean searchFieldItemExists(String fieldName);

    // sort related methods
    SearchFieldItem getCorrespondingSortField(String searchFieldName);

    boolean correspondingSortFieldExists(String searchFieldName);

    List<SearchFieldItem> getSortFieldItems();
}
