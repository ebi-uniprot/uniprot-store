package org.uniprot.store.config.searchfield.impl;

import org.uniprot.store.config.searchfield.common.AbstractSearchFieldConfig;
import org.uniprot.store.config.searchfield.common.SearchFieldConfig;

public class UniParcSearchFieldConfiguration extends AbstractSearchFieldConfig {
    public static final String CONFIG_FILE = "search-fields-config/uniparc-search-fields.json";

    private UniParcSearchFieldConfiguration() {
        super(SCHEMA_FILE, CONFIG_FILE);
    }

    private static class SearchFieldConfigurationHolder {
        private static final SearchFieldConfig INSTANCE = new UniParcSearchFieldConfiguration();
    }

    public static SearchFieldConfig getInstance() {
        return SearchFieldConfigurationHolder.INSTANCE;
    }
}
