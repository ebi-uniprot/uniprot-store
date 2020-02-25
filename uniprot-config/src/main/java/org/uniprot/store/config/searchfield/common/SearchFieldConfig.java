package org.uniprot.store.config.searchfield.common;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.uniprot.store.config.searchfield.model.FieldItem;
import org.uniprot.store.config.searchfield.model.FieldType;

public interface SearchFieldConfig {
    String SCHEMA_FILE = "schema/search-fields-schema.json";

    // file load and read related methods
    List<FieldItem> loadAndGetFieldItems(String config);

    InputStream readConfig(String config);

    Map<String, FieldItem> buildIdFieldItemMap(List<FieldItem> fieldItems);

    // common methods
    List<FieldItem> getAllFieldItems();
    FieldType getFieldTypeByFieldName(String fieldName);

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
