package org.uniprot.store.config.service.impl;

import org.uniprot.store.config.service.ConfigFieldService;
import org.uniprot.store.config.repository.ConfigFieldRepository;
import org.uniprot.store.config.model.FieldItem;
import org.uniprot.store.config.repository.impl.UniProtConfigFieldRepositoryImpl;

import java.util.List;

public class UniProtConfigFieldServiceImpl implements ConfigFieldService {
    private ConfigFieldRepository repository;

    public UniProtConfigFieldServiceImpl() {
        this.repository = UniProtConfigFieldRepositoryImpl.getInstance();
    }

    @Override
    public List<FieldItem> getAllFieldItems() {
        return this.repository.getFieldItems();
    }

    @Override
    public FieldItem getFieldItemById(String id) {
        return this.repository.getIdFieldItemMap().get(id);
    }
}
