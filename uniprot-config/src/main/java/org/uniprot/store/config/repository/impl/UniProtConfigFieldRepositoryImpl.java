package org.uniprot.store.config.repository.impl;

import org.uniprot.store.config.repository.AbstractConfigFieldRepository;
import org.uniprot.store.config.repository.ConfigFieldRepository;

public final class UniProtConfigFieldRepositoryImpl extends AbstractConfigFieldRepository {
    private static final String CONFIG_FILE = "uniprot-fields.json";
    private static final String SCHEMA_FILE = "fields-schema.json";

    private UniProtConfigFieldRepositoryImpl() {
        super(SCHEMA_FILE, CONFIG_FILE);
    }

    public static ConfigFieldRepository getInstance() {
        return ConfigFieldRepositoryHolder.INSTANCE;
    }

    private static class ConfigFieldRepositoryHolder {
        private static final ConfigFieldRepository INSTANCE = new UniProtConfigFieldRepositoryImpl();
    }
}
