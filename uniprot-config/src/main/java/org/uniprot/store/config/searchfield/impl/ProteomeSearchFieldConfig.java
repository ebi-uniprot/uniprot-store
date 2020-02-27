package org.uniprot.store.config.searchfield.impl;

import org.uniprot.store.config.searchfield.common.AbstractSearchFieldConfig;
import org.uniprot.store.config.searchfield.common.SearchFieldConfig;

public class ProteomeSearchFieldConfig extends AbstractSearchFieldConfig {
    public static final String CONFIG_FILE = "search-fields-config/proteome-search-fields.json";

    private ProteomeSearchFieldConfig() {
        super(SCHEMA_FILE, CONFIG_FILE);
    }

    private static class SearchFieldConfigHolder {
        private static final SearchFieldConfig INSTANCE = new ProteomeSearchFieldConfig();
    }

    public static SearchFieldConfig getInstance() {
        return SearchFieldConfigHolder.INSTANCE;
    }
}
