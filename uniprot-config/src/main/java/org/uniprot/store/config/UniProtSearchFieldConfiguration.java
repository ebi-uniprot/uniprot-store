package org.uniprot.store.config;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.NonNull;

import org.uniprot.store.schema.SchemaValidator;

public class UniProtSearchFieldConfiguration extends AbstractFieldConfiguration {
    public static final String CONFIG_FILE = "uniprot-fields.json";
    public static final String SCHEMA_FILE = "fields-schema.json";
    private List<FieldItem> fieldItems;
    private Map<String, FieldItem> idFieldItemMap;

    private UniProtSearchFieldConfiguration() {
        SchemaValidator.validate(SCHEMA_FILE, CONFIG_FILE);
        init();
    }

    private static class SearchFieldConfigurationHolder {
        private static final UniProtSearchFieldConfiguration INSTANCE =
                new UniProtSearchFieldConfiguration();
    }

    public static UniProtSearchFieldConfiguration getInstance() {
        return SearchFieldConfigurationHolder.INSTANCE;
    }

    public void init() {
        this.fieldItems = loadAndGetFieldItems(CONFIG_FILE);
        this.idFieldItemMap = buildIdFieldItemMap(this.fieldItems);
    }

    public List<FieldItem> getAllFieldItems() {
        return this.fieldItems;
    }

    public FieldItem getFieldItemById(@NonNull String id) {
        return this.idFieldItemMap.get(id);
    }

    @Override
    public List<FieldItem> getTopLevelFieldItems() {
        return this.fieldItems.stream().filter(fi -> isTopLevel(fi)).collect(Collectors.toList());
    }

    @Override
    public List<FieldItem> getChildFieldItems(String parentId) {
        return this.fieldItems.stream()
                .filter(fi -> isChildOf(parentId, fi))
                .collect(Collectors.toList());
    }

    private boolean isChildOf(String parentId, FieldItem fieldItem) {
        return parentId.equals(fieldItem.getParentId());
    }

    private boolean isTopLevel(FieldItem fi) {
        return (fi.getParentId() == null || fi.getParentId().isEmpty())
                && fi.getSeqNumber()
                        != null; // if seqNumber is set, it means it is used by advance search
    }

    void initForTesting(String testConfigFile) {
        this.fieldItems = loadAndGetFieldItems(testConfigFile);
        this.idFieldItemMap = buildIdFieldItemMap(this.fieldItems);
    }
}
