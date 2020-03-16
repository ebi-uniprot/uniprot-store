package org.uniprot.store.config.searchfield.impl;

import org.uniprot.core.cv.xdb.UniProtDatabaseDetail;
import org.uniprot.cv.xdb.UniProtDatabaseTypes;
import org.uniprot.store.config.UniProtDataType;
import org.uniprot.store.config.searchfield.common.AbstractSearchFieldConfig;
import org.uniprot.store.config.searchfield.model.SearchFieldDataType;
import org.uniprot.store.config.searchfield.model.SearchFieldItem;
import org.uniprot.store.config.searchfield.model.SearchFieldType;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;

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
        AtomicInteger childNumber = new AtomicInteger();
        return UniProtDatabaseTypes.INSTANCE.getAllDbTypes().stream()
                .map(field -> convertToFieldItem(field, childNumber))
                .collect(Collectors.toList());
    }

    private SearchFieldItem convertToFieldItem(UniProtDatabaseDetail db, AtomicInteger childNumber) {
        String fieldName = XREF_COUNT_PREFIX + db.getName().toLowerCase();
        SearchFieldItem fieldItem = new SearchFieldItem();
        fieldItem.setId(fieldName);
        fieldItem.setParentId("cross_references");
        fieldItem.setChildNumber(childNumber.getAndIncrement());
        fieldItem.setFieldName(fieldName);
        fieldItem.setFieldType(SearchFieldType.RANGE);
        fieldItem.setDataType(SearchFieldDataType.INTEGER);
        return fieldItem;
    }
}
