package org.uniprot.store.config.searchfield.impl;

import java.util.List;
import java.util.stream.Collectors;

import org.uniprot.core.cv.xdb.UniProtDatabaseDetail;
import org.uniprot.cv.xdb.UniProtDatabaseTypes;
import org.uniprot.store.config.searchfield.common.AbstractSearchFieldConfig;
import org.uniprot.store.config.UniProtDataType;
import org.uniprot.store.config.searchfield.model.SearchFieldDataType;
import org.uniprot.store.config.searchfield.model.SearchFieldItem;
import org.uniprot.store.config.searchfield.model.SearchFieldType;

public class SearchFieldConfigImpl extends AbstractSearchFieldConfig {
    private static final String XREF_COUNT_PREFIX = "xref_count_";
    private UniProtDataType dataType;

    public SearchFieldConfigImpl(UniProtDataType dataType, String configFile) {
        super(SCHEMA_FILE, configFile);
        this.dataType = dataType;
        if (UniProtDataType.UNIPROTKB == this.dataType) { // add db xref related count fields
            List<SearchFieldItem> crossRefSearchItems = getCrossRefCountSearchFieldItems();
            addSearchFieldItems(crossRefSearchItems);
        }
    }

    private List<SearchFieldItem> getCrossRefCountSearchFieldItems() {
        return UniProtDatabaseTypes.INSTANCE.getAllDbTypes().stream()
                .map(this::convertToFieldItem)
                .collect(Collectors.toList());
    }

    private SearchFieldItem convertToFieldItem(UniProtDatabaseDetail db) {
        String fieldName = XREF_COUNT_PREFIX + db.getName().toLowerCase();
        SearchFieldItem fieldItem = new SearchFieldItem();
        fieldItem.setId(fieldName);
        fieldItem.setFieldName(fieldName);
        fieldItem.setFieldType(SearchFieldType.RANGE);
        fieldItem.setDataType(SearchFieldDataType.INTEGER);
        return fieldItem;
    }
}
