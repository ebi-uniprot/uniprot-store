package org.uniprot.store.config.searchfield.impl;

import org.uniprot.store.config.searchfield.common.AbstractSearchFieldConfig;
import org.uniprot.store.config.searchfield.common.SearchFieldConfig;

public class CrossRefSearchFieldConfiguration extends AbstractSearchFieldConfig {
    public static final String CONFIG_FILE = "search-fields-config/crossref-search-fields.json";

    private CrossRefSearchFieldConfiguration() {
        super(SCHEMA_FILE, CONFIG_FILE);
    }

    private static class SearchFieldConfigurationHolder {
        private static final SearchFieldConfig INSTANCE = new CrossRefSearchFieldConfiguration();
    }

    public static SearchFieldConfig getInstance() {
        return SearchFieldConfigurationHolder.INSTANCE;
    }
}
