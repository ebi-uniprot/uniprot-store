package org.uniprot.store.config.service.impl;

import org.uniprot.store.config.repository.impl.UniProtConfigFieldRepositoryImpl;
import org.uniprot.store.config.service.AbstractConfigFieldService;
import org.uniprot.store.config.service.ConfigFieldService;

public final class UniProtConfigFieldServiceImpl extends AbstractConfigFieldService {

    private UniProtConfigFieldServiceImpl() {
        super(UniProtConfigFieldRepositoryImpl.getInstance());
    }

    public static ConfigFieldService getInstance() {
        return UniProtConfigFieldServiceImpl.ConfigFieldServiceHolder.INSTANCE;
    }

    private static class ConfigFieldServiceHolder {
        private static final ConfigFieldService INSTANCE = new UniProtConfigFieldServiceImpl();
    }
}
