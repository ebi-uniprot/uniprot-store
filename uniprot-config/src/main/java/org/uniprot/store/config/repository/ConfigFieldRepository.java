package org.uniprot.store.config.repository;

import lombok.NonNull;
import org.uniprot.store.config.model.ConfigFieldItem;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

public interface ConfigFieldRepository {
    void init();
    List<ConfigFieldItem> loadAndGetFieldItems(@NonNull String config);
    Map<String, ConfigFieldItem> buildIdFieldItemMap(@NonNull List<ConfigFieldItem> fieldItems);
    InputStream readConfig(String config);
    List<ConfigFieldItem> getFieldItems();
    Map<String, ConfigFieldItem> getIdFieldItemMap();
}
