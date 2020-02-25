package org.uniprot.store.config.searchfield.common;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.uniprot.store.config.searchfield.model.FieldItem;

public interface SearchFieldConfig {
    String SCHEMA_FILE = "schema/search-fields-schema.json";

    List<FieldItem> loadAndGetFieldItems(String config);

    Map<String, FieldItem> buildIdFieldItemMap(List<FieldItem> fieldItems);

    List<FieldItem> getAllFieldItems();

    FieldItem getFieldItemById(String id);

    List<FieldItem> getSearchFieldItems();

    FieldItem getSearchFieldItemByName(String fieldName);

    Boolean isSearchFieldValueValid(String fieldName, String value);

    Boolean hasSearchFieldItem(String fieldName);

    FieldItem getCorrespondingSortField(String searchFieldName);

    Boolean hasCorrespondingSortField(String searchFieldName);

    List<FieldItem> getSortFieldItems();

    Optional<FieldItem> getSortFieldItemByName(String fieldName);

    Boolean hasSortFieldItem(String sortFieldName);

    List<FieldItem> getTopLevelFieldItems();

    List<FieldItem> getChildFieldItems(String parentId);

    InputStream readConfig(String config);
}
