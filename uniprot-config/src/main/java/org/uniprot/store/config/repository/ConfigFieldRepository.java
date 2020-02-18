package org.uniprot.store.config.repository;

import lombok.NonNull;
import org.uniprot.store.config.model.FieldItem;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

public interface ConfigFieldRepository {
    void init();
    List<FieldItem> loadAndGetFieldItems(@NonNull String config);
    Map<String, FieldItem> buildIdFieldItemMap(@NonNull List<FieldItem> fieldItems);
    InputStream readConfig(String config);
    List<FieldItem> getFieldItems();
    Map<String, FieldItem> getIdFieldItemMap();
}
