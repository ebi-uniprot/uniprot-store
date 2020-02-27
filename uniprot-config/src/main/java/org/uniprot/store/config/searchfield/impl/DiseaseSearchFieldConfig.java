package org.uniprot.store.config.searchfield.impl;

import org.uniprot.store.config.searchfield.common.AbstractSearchFieldConfig;
import org.uniprot.store.config.searchfield.common.SearchFieldConfig;

public class DiseaseSearchFieldConfig extends AbstractSearchFieldConfig {
    public static final String CONFIG_FILE = "search-fields-config/disease-search-fields.json";

    private DiseaseSearchFieldConfig() {
        super(SCHEMA_FILE, CONFIG_FILE);
    }

    private static class SearchFieldConfigHolder {
        private static final SearchFieldConfig INSTANCE = new DiseaseSearchFieldConfig();
    }

    public static SearchFieldConfig getInstance() {
        return SearchFieldConfigHolder.INSTANCE;
    }
}
