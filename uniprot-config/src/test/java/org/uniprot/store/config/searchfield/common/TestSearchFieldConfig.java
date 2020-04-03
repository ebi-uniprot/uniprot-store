package org.uniprot.store.config.searchfield.common;

import static java.util.Collections.singletonList;

import java.util.Collection;

import org.uniprot.store.config.UniProtDataType;
import org.uniprot.store.config.searchfield.model.SearchFieldItem;
import org.uniprot.store.config.searchfield.model.SearchFieldItemType;

public class TestSearchFieldConfig extends AbstractSearchFieldConfig {
    public static final String TEST_SEARCH_FIELDS_CONFIG = "test-uniprotkb-search-fields.json";

    public static final String TEST_SCHEMA_CONFIG =
            "src/test/resources/test-search-fields-schema.json";

    private TestSearchFieldConfig() {
        super(UniProtDataType.UNIPROTKB, TEST_SCHEMA_CONFIG, TEST_SEARCH_FIELDS_CONFIG);
    }

    public static SearchFieldConfig getInstance() {
        return SearchFieldConfigHolder.INSTANCE;
    }

    @Override
    protected Collection<SearchFieldItem> dynamicallyLoadFields() {
        SearchFieldItem field = new SearchFieldItem();
        field.setItemType(SearchFieldItemType.SINGLE);
        field.setParentId("cross_references");
        field.setId("a dynamic field");
        field.setChildNumber(0);
        return singletonList(field);
    }

    private static class SearchFieldConfigHolder {
        private static final SearchFieldConfig INSTANCE = new TestSearchFieldConfig();
    }
}
