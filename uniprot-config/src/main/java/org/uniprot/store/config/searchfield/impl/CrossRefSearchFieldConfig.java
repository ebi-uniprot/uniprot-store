package org.uniprot.store.config.searchfield.impl;

import org.uniprot.store.config.searchfield.common.AbstractSearchFieldConfig;
import org.uniprot.store.config.searchfield.common.SearchFieldConfig;

public class CrossRefSearchFieldConfig extends AbstractSearchFieldConfig {
    public static final String CONFIG_FILE = "search-fields-config/crossref-search-fields.json";

    private CrossRefSearchFieldConfig() {
        super(SCHEMA_FILE, CONFIG_FILE);
    }

    private static class SearchFieldConfigHolder {
        private static final SearchFieldConfig INSTANCE = new CrossRefSearchFieldConfig();
    }

    public static SearchFieldConfig getInstance() {
        return SearchFieldConfigHolder.INSTANCE;
    }
}
