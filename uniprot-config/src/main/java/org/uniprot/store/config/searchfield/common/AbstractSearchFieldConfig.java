package org.uniprot.store.config.searchfield.common;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.StringUtils;
import org.uniprot.store.config.searchfield.model.FieldItem;
import org.uniprot.store.config.searchfield.model.FieldType;
import org.uniprot.store.config.searchfield.schema.DataValidator;
import org.uniprot.store.config.searchfield.schema.SchemaValidator;

@Slf4j
public abstract class AbstractSearchFieldConfig implements SearchFieldConfig {
    public static String SCHEMA_FILE = "schema/search-fields-schema.json";

    private List<FieldItem> fieldItems;
    private List<FieldItem> searchFieldItems;
    private List<FieldItem> sortFieldItems;
    private Map<String, FieldItem> idFieldItemMap;
    private String schemaFile;
    private String configFile;
    private SearchFieldConfigLoader loader;

    protected AbstractSearchFieldConfig(String schemaFile, String configFile) {
        this.loader = new SearchFieldConfigLoader();
        SchemaValidator.validate(schemaFile, configFile);
        init(schemaFile, configFile);
        DataValidator.validateContent(this.fieldItems, idFieldItemMap);
    }

    private void init(String schemaFile, String configFile) {
        this.schemaFile = schemaFile;
        this.configFile = configFile;
        this.fieldItems = loader.loadAndGetFieldItems(this.configFile);
        this.idFieldItemMap = loader.buildIdFieldItemMap(this.fieldItems);
    }

    public List<FieldItem> getAllFieldItems() {
        return this.fieldItems;
    }

    public List<FieldItem> getSearchFieldItems() {
        if (this.searchFieldItems == null) {
            this.searchFieldItems =
                    getAllFieldItems().stream()
                            .filter(this::isSearchFieldItem)
                            .collect(Collectors.toList());
        }
        return this.searchFieldItems;
    }

    public FieldItem getSearchFieldItemByName(String fieldName) {
        return this.getSearchFieldItems().stream()
                .filter(fi -> fieldName.equals(fi.getFieldName()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown field: " + fieldName));
    }

    @Override
    public Boolean isSearchFieldValueValid(String fieldName, String value) {
        FieldItem searchField = this.getSearchFieldItemByName(fieldName);
        String validRegex = searchField.getValidRegex();
        if (StringUtils.isNotEmpty(validRegex)) {
            return value.matches(validRegex);
        } else {
            return true;
        }
    }

    @Override
    public Boolean doesSearchFieldItemExist(String fieldName) {
        Boolean searchFieldExist = false;
        try {
            searchFieldExist = Objects.nonNull(this.getSearchFieldItemByName(fieldName));
        } catch (IllegalArgumentException ile) {
            // it means, search field doesn't exist
        }
        return searchFieldExist;
    }

    @Override
    public FieldItem getCorrespondingSortField(String searchFieldName) {
        FieldItem searchField = getSearchFieldItemByName(searchFieldName);
        String sortFieldId = searchField.getSortFieldId();
        return getSortFieldItems().stream()
                .filter(sortFieldItem -> sortFieldItem.getId().equals(sortFieldId))
                .findFirst()
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        "Field '"
                                                + searchFieldName
                                                + "' does not have an associated sort field."));
    }

    @Override
    public Boolean doesCorrespondingSortFieldExist(String searchFieldName) {
        Boolean sortFieldExist = false;
        try {
            sortFieldExist = Objects.nonNull(getCorrespondingSortField(searchFieldName));
        } catch (IllegalArgumentException ile) {
            // it means, sort field doesn't exist
        }
        return sortFieldExist;
    }

    public List<FieldItem> getSortFieldItems() {
        if (this.sortFieldItems == null) {
            this.sortFieldItems =
                    getAllFieldItems().stream()
                            .filter(this::isSortFieldItem)
                            .collect(Collectors.toList());
        }
        return this.sortFieldItems;
    }

    public FieldType getFieldTypeBySearchFieldName(String fieldName) {
        FieldItem fieldItem = getSearchFieldItemByName(fieldName);
        if (fieldItem.getFieldType() == FieldType.evidence) {
            return FieldType.general;
        }
        return fieldItem.getFieldType();
    }

    protected void addSearchFieldItems(List<FieldItem> searchFieldItems) {
        if (this.searchFieldItems == null) {
            this.searchFieldItems = getSearchFieldItems();
        }
        this.searchFieldItems.addAll(searchFieldItems);
    }

    private boolean isSearchFieldItem(FieldItem fieldItem) {
        return Objects.nonNull(fieldItem.getFieldType())
                && FieldType.sort != fieldItem.getFieldType();
    }

    private boolean isSortFieldItem(FieldItem fieldItem) {
        return FieldType.sort.equals(fieldItem.getFieldType());
    }
}
