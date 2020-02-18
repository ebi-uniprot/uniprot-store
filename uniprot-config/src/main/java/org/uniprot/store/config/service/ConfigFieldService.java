package org.uniprot.store.config.service;

import org.uniprot.store.config.model.ConfigFieldItem;

import java.util.List;
import java.util.Optional;

public interface ConfigFieldService {
    List<ConfigFieldItem> getAllFieldItems();
    List<ConfigFieldItem> getSearchFieldItems();
    Optional<ConfigFieldItem> getSearchFieldItemByName(String fieldName);
    boolean hasSearchFieldItem(String fieldName);
    boolean isSearchFieldValueValid(String fieldName, String value);
    List<ConfigFieldItem> getSortFieldItems();
    Optional<ConfigFieldItem> getSortFieldItemByName(String fieldName);
    boolean hasSortFieldItem(String fieldName);
}
