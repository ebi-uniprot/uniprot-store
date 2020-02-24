package org.uniprot.store.config.searchfield.impl;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.uniprot.store.config.searchfield.common.AbstractSearchFieldConfig;
import org.uniprot.store.config.searchfield.common.SearchFieldConfig;
import org.uniprot.store.config.searchfield.model.FieldItem;

public class UniProtKBSearchFieldConfiguration extends AbstractSearchFieldConfig {
    public static final String CONFIG_FILE = "search-fields-config/uniprotkb-search-fields.json";

    private UniProtKBSearchFieldConfiguration() {
        super(SCHEMA_FILE, CONFIG_FILE);
    }

    private static class SearchFieldConfigurationHolder {
        private static final SearchFieldConfig INSTANCE = new UniProtKBSearchFieldConfiguration();
    }

    public static SearchFieldConfig getInstance() {
        return SearchFieldConfigurationHolder.INSTANCE;
    }

    @Override
    public List<FieldItem> getTopLevelFieldItems() {
        return this.getAllFieldItems().stream()
                .filter(this::isTopLevel)
                .collect(Collectors.toList());
    }

    @Override
    public List<FieldItem> getChildFieldItems(String parentId) {
        return this.getAllFieldItems().stream()
                .filter(fi -> isChildOf(parentId, fi))
                .collect(Collectors.toList());
    }

    private boolean isChildOf(String parentId, FieldItem fieldItem) {
        return parentId.equals(fieldItem.getParentId());
    }

    private boolean isTopLevel(FieldItem fi) {
        return StringUtils.isBlank(fi.getParentId()) && fi.getSeqNumber() != null;
    }
}
