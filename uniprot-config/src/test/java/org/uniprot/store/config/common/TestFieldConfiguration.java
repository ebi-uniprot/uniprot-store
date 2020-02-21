package org.uniprot.store.config.common;

public class TestFieldConfiguration extends AbstractSearchFieldConfiguration {
    public static final String TEST_SEARCH_FIELDS_CONFIG =
            "src/test/resources/test-uniprotkb-search-fields.json";

    public static final String TEST_SCHEMA_CONFIG =
            "src/test/resources/test-search-fields-schema.json";

    private TestFieldConfiguration() {
        super(TEST_SCHEMA_CONFIG, TEST_SEARCH_FIELDS_CONFIG);
    }

    public static SearchFieldConfiguration getInstance() {
        return SearchFieldConfigurationHolder.INSTANCE;
    }

    private static class SearchFieldConfigurationHolder {
        private static final SearchFieldConfiguration INSTANCE = new TestFieldConfiguration();
    }
}
