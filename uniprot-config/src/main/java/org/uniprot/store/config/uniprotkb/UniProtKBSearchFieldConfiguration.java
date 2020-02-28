package org.uniprot.store.config.uniprotkb;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.uniprot.store.config.common.AbstractFieldConfiguration;
import org.uniprot.store.config.common.FieldConfiguration;
import org.uniprot.store.config.model.FieldItem;

public class UniProtKBSearchFieldConfiguration extends AbstractFieldConfiguration {
    public static final String CONFIG_FILE = "uniprot-fields.json";
    public static final String SCHEMA_FILE = "fields-schema.json";

    private UniProtKBSearchFieldConfiguration() {
        super(SCHEMA_FILE, CONFIG_FILE);
    }

    private static class SearchFieldConfigurationHolder {
        private static final FieldConfiguration INSTANCE = new UniProtKBSearchFieldConfiguration();
    }

    public static FieldConfiguration getInstance() {
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
