package org.uniprot.store.config.searchfield.impl;

import org.uniprot.store.config.searchfield.common.AbstractSearchFieldConfig;
import org.uniprot.store.config.searchfield.common.SearchFieldConfig;

public class SubcellLocationSearchFieldConfiguration extends AbstractSearchFieldConfig {
    public static final String CONFIG_FILE =
            "search-fields-config/subcell-location-search-fields.json";

    private SubcellLocationSearchFieldConfiguration() {
        super(SCHEMA_FILE, CONFIG_FILE);
    }

    private static class SearchFieldConfigurationHolder {
        private static final SearchFieldConfig INSTANCE =
                new SubcellLocationSearchFieldConfiguration();
    }

    public static SearchFieldConfig getInstance() {
        return SearchFieldConfigurationHolder.INSTANCE;
    }
}
