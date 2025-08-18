package org.uniprot.store.config.searchfield.impl;

import static java.util.Collections.emptyList;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.uniprot.core.cv.xdb.UniProtDatabaseDetail;
import org.uniprot.cv.xdb.UniProtDatabaseTypes;
import org.uniprot.store.config.UniProtDataType;
import org.uniprot.store.config.searchfield.common.AbstractSearchFieldConfig;
import org.uniprot.store.config.searchfield.model.SearchFieldDataType;
import org.uniprot.store.config.searchfield.model.SearchFieldItem;
import org.uniprot.store.config.searchfield.model.SearchFieldType;

public class SearchFieldConfigImpl extends AbstractSearchFieldConfig {
    private static final String XREF_COUNT_PREFIX = "xref_count_";

    public SearchFieldConfigImpl(UniProtDataType dataType, String configFile) {
        super(dataType, SCHEMA_FILE, configFile);
    }

    @Override
    protected Collection<SearchFieldItem> dynamicallyLoadFields() {
        if (UniProtDataType.UNIPROTKB == dataType) { // add db xref related count fields
            return getCrossRefCountSearchFieldItems();
        } else {
            return emptyList();
        }
    }

    private List<SearchFieldItem> getCrossRefCountSearchFieldItems() {
        return UniProtDatabaseTypes.INSTANCE.getUniProtKBDbTypes().stream()
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
