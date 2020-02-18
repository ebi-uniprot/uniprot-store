package org.uniprot.store.config.service;

import org.uniprot.store.config.model.FieldItem;

import java.util.List;

public interface ConfigFieldService {
    List<FieldItem> getAllFieldItems();
    FieldItem getFieldItemById(String id);
}
