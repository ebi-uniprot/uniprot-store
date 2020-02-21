package org.uniprot.store.config.uniprotkb;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.uniprot.store.config.common.AbstractSearchFieldConfiguration;
import org.uniprot.store.config.common.SearchFieldConfiguration;
import org.uniprot.store.config.model.FieldItem;

public class UniProtKBSearchFieldConfiguration extends AbstractSearchFieldConfiguration {
    public static final String CONFIG_FILE = "uniprotkb-search-fields.json";

    private UniProtKBSearchFieldConfiguration() {
        super(SCHEMA_FILE, CONFIG_FILE);
    }

    private static class SearchFieldConfigurationHolder {
        private static final SearchFieldConfiguration INSTANCE =
                new UniProtKBSearchFieldConfiguration();
    }

    public static SearchFieldConfiguration getInstance() {
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
