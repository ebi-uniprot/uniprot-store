package org.uniprot.store.config.common;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.uniprot.store.config.model.FieldItem;

public interface FieldConfiguration {
    void init(String schemaFile, String configFile);

    List<FieldItem> loadAndGetFieldItems(String config);

    Map<String, FieldItem> buildIdFieldItemMap(List<FieldItem> fieldItems);

    List<FieldItem> getAllFieldItems();

    FieldItem getFieldItemById(String id);

    List<FieldItem> getTopLevelFieldItems();

    List<FieldItem> getChildFieldItems(String parentId);

    InputStream readConfig(String config);
}
