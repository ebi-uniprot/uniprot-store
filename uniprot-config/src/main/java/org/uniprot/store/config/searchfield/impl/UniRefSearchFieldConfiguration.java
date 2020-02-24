package org.uniprot.store.config.searchfield.impl;

import org.uniprot.store.config.searchfield.common.AbstractSearchFieldConfig;
import org.uniprot.store.config.searchfield.common.SearchFieldConfig;

public class UniRefSearchFieldConfiguration extends AbstractSearchFieldConfig {
    public static final String CONFIG_FILE = "search-fields-config/uniref-search-fields.json";

    private UniRefSearchFieldConfiguration() {
        super(SCHEMA_FILE, CONFIG_FILE);
    }

    private static class SearchFieldConfigurationHolder {
        private static final SearchFieldConfig INSTANCE = new UniRefSearchFieldConfiguration();
    }

    public static SearchFieldConfig getInstance() {
        return SearchFieldConfigurationHolder.INSTANCE;
    }
}
