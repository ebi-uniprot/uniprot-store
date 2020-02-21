package org.uniprot.store.config.crossref;

import org.uniprot.store.config.common.AbstractSearchFieldConfiguration;
import org.uniprot.store.config.common.SearchFieldConfiguration;

public class CrossRefSearchFieldConfiguration extends AbstractSearchFieldConfiguration {
    public static final String CONFIG_FILE = "crossref-search-fields.json";

    private CrossRefSearchFieldConfiguration() {
        super(SCHEMA_FILE, CONFIG_FILE);
    }

    private static class SearchFieldConfigurationHolder {
        private static final SearchFieldConfiguration INSTANCE =
                new CrossRefSearchFieldConfiguration();
    }

    public static SearchFieldConfiguration getInstance() {
        return SearchFieldConfigurationHolder.INSTANCE;
    }
}
