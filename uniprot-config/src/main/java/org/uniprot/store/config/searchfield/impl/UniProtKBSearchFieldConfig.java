package org.uniprot.store.config.searchfield.impl;

import java.util.List;
import java.util.stream.Collectors;

import org.uniprot.core.cv.xdb.UniProtXDbTypeDetail;
import org.uniprot.cv.xdb.UniProtXDbTypes;
import org.uniprot.store.config.searchfield.common.AbstractSearchFieldConfig;
import org.uniprot.store.config.searchfield.common.SearchFieldConfig;
import org.uniprot.store.config.searchfield.model.SearchFieldDataType;
import org.uniprot.store.config.searchfield.model.SearchFieldItem;
import org.uniprot.store.config.searchfield.model.SearchFieldType;

public class UniProtKBSearchFieldConfig extends AbstractSearchFieldConfig {
    public static final String CONFIG_FILE = "search-fields-config/uniprotkb-search-fields.json";
    private static final String XREF_COUNT_PREFIX = "xref_count_";

    private UniProtKBSearchFieldConfig() {
        super(SCHEMA_FILE, CONFIG_FILE);
        // add db xref related count fields
        List<SearchFieldItem> crossRefSearchItems = getCrossRefCountSearchFieldItems();
        addSearchFieldItems(crossRefSearchItems);
    }

    private static class SearchFieldConfigHolder {
        private static final SearchFieldConfig INSTANCE = new UniProtKBSearchFieldConfig();
    }

    public static SearchFieldConfig getInstance() {
        return SearchFieldConfigHolder.INSTANCE;
    }

    private List<SearchFieldItem> getCrossRefCountSearchFieldItems() {
        return UniProtXDbTypes.INSTANCE.getAllDBXRefTypes().stream()
                .map(db -> convertToFieldItem(db))
                .collect(Collectors.toList());
    }

    private SearchFieldItem convertToFieldItem(UniProtXDbTypeDetail db) {
        String fieldName = XREF_COUNT_PREFIX + db.getName().toLowerCase();
        SearchFieldItem fieldItem = new SearchFieldItem();
        fieldItem.setId(fieldName);
        fieldItem.setFieldName(fieldName);
        fieldItem.setFieldType(SearchFieldType.range);
        fieldItem.setDataType(SearchFieldDataType.integer);
        return fieldItem;
    }
}
