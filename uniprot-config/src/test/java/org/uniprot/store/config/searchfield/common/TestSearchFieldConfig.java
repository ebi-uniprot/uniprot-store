package org.uniprot.store.config.searchfield.common;

public class TestSearchFieldConfig extends AbstractSearchFieldConfig {
    public static final String TEST_SEARCH_FIELDS_CONFIG =
            "src/test/resources/test-uniprotkb-search-fields.json";

    public static final String TEST_SCHEMA_CONFIG =
            "src/test/resources/test-search-fields-schema.json";

    private TestSearchFieldConfig() {
        super(TEST_SCHEMA_CONFIG, TEST_SEARCH_FIELDS_CONFIG);
    }

    public static SearchFieldConfig getInstance() {
        return SearchFieldConfigHolder.INSTANCE;
    }

    private static class SearchFieldConfigHolder {
        private static final SearchFieldConfig INSTANCE = new TestSearchFieldConfig();
    }
}
