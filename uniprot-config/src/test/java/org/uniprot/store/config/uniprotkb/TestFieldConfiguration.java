//package org.uniprot.store.config.uniprotkb;
//
//import org.uniprot.store.config.common.AbstractFieldConfiguration;
//import org.uniprot.store.config.common.FieldConfiguration;
//
//public class TestFieldConfiguration extends AbstractFieldConfiguration {
//    private static final String TEST_SEARCH_FIELDS_CONFIG = "src/test/resources/test-uniprot-fields.json";
//    private static final String TEST_SCHEMA_CONFIG = "src/test/resources/test-schema.json";
//
//    public TestFieldConfiguration(){
//        super(TEST_SCHEMA_CONFIG, TEST_SEARCH_FIELDS_CONFIG);
//    }
//
//    private static class SearchFieldConfigurationHolder {
//        private static final FieldConfiguration INSTANCE = new TestFieldConfiguration();
//    }
//
//    public static FieldConfiguration getInstance() {
//        return TestFieldConfiguration.SearchFieldConfigurationHolder.INSTANCE;
//    }
//
//
//}
