package org.uniprot.store.config.repository.impl;

import org.uniprot.store.config.repository.AbstractConfigFieldRepository;
import org.uniprot.store.config.repository.ConfigFieldRepository;

public final class UniProtConfigFieldRepositoryImpl extends AbstractConfigFieldRepository {
    public static final String CONFIG_FILE = "uniprot-fields.json";
    public static final String SCHEMA_FILE = "fields-schema.json";

    private UniProtConfigFieldRepositoryImpl() {
        super(SCHEMA_FILE, CONFIG_FILE);
    }

    private static class FieldRepositoryHolder {
        private static final ConfigFieldRepository INSTANCE = new UniProtConfigFieldRepositoryImpl();
    }

    public static ConfigFieldRepository getInstance() {
        return FieldRepositoryHolder.INSTANCE;
    }
}
