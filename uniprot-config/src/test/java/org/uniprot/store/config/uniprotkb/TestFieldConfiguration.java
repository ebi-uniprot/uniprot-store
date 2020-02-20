package org.uniprot.store.config.uniprotkb;

import org.uniprot.store.config.common.AbstractFieldConfiguration;
import org.uniprot.store.config.common.FieldConfiguration;

public class TestFieldConfiguration extends AbstractFieldConfiguration {
    public static final String TEST_SEARCH_FIELDS_CONFIG =
            "src/test/resources/test-uniprot-fields.json";

    public static final String TEST_SCHEMA_CONFIG = "src/test/resources/test-schema.json";

    private TestFieldConfiguration() {
        super(TEST_SCHEMA_CONFIG, TEST_SEARCH_FIELDS_CONFIG);
    }

    public static FieldConfiguration getInstance() {
        return SearchFieldConfigurationHolder.INSTANCE;
    }

    private static class SearchFieldConfigurationHolder {
        private static final FieldConfiguration INSTANCE = new TestFieldConfiguration();
    }
}
