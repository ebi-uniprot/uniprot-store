package org.uniprot.store.config.searchfield.common;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.StringUtils;
import org.uniprot.store.config.searchfield.model.SearchFieldItem;
import org.uniprot.store.config.searchfield.model.SearchFieldType;
import org.uniprot.store.config.searchfield.schema.DataValidator;
import org.uniprot.store.config.searchfield.schema.SchemaValidator;

@Slf4j
public abstract class AbstractSearchFieldConfig implements SearchFieldConfig {
    public static final String SCHEMA_FILE = "schema/search-fields-schema.json";

    private List<SearchFieldItem> fieldItems;
    private List<SearchFieldItem> searchFieldItems;
    private List<SearchFieldItem> sortFieldItems;
    private Map<String, SearchFieldItem> idFieldItemMap;
    private String schemaFile;
    private String configFile;
    private final SearchFieldConfigLoader loader;

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

    public List<SearchFieldItem> getAllFieldItems() {
        return this.fieldItems;
    }

    public List<SearchFieldItem> getSearchFieldItems() {
        if (this.searchFieldItems == null) {
            this.searchFieldItems =
                    getAllFieldItems().stream()
                            .filter(this::isSearchFieldItem)
                            .collect(Collectors.toList());
        }
        return this.searchFieldItems;
    }

    public SearchFieldItem getSearchFieldItemByName(String fieldName) {
        return this.getSearchFieldItems().stream()
                .filter(fi -> fieldName.equals(fi.getFieldName()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown field: " + fieldName));
    }

    @Override
    public boolean isSearchFieldValueValid(String fieldName, String value) {
        SearchFieldItem searchField = this.getSearchFieldItemByName(fieldName);
        String validRegex = searchField.getValidRegex();
        if (StringUtils.isNotEmpty(validRegex)) {
            return value.matches(validRegex);
        } else {
            return true;
        }
    }

    @Override
    public boolean searchFieldItemExists(String fieldName) {
        boolean searchFieldExist = false;
        try {
            searchFieldExist = Objects.nonNull(this.getSearchFieldItemByName(fieldName));
        } catch (IllegalArgumentException ile) {
            // it means, search field doesn't exist
        }
        return searchFieldExist;
    }

    @Override
    public SearchFieldItem getCorrespondingSortField(String searchFieldName) {
        SearchFieldItem searchField = getSearchFieldItemByName(searchFieldName);
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
    public boolean correspondingSortFieldExists(String searchFieldName) {
        boolean sortFieldExist = false;
        try {
            sortFieldExist = Objects.nonNull(getCorrespondingSortField(searchFieldName));
        } catch (IllegalArgumentException ile) {
            // it means, sort field doesn't exist
        }
        return sortFieldExist;
    }

    public List<SearchFieldItem> getSortFieldItems() {
        if (this.sortFieldItems == null) {
            this.sortFieldItems =
                    getAllFieldItems().stream()
                            .filter(this::isSortFieldItem)
                            .collect(Collectors.toList());
        }
        return this.sortFieldItems;
    }

    public SearchFieldType getFieldTypeBySearchFieldName(String fieldName) {
        SearchFieldItem fieldItem = getSearchFieldItemByName(fieldName);
        if (fieldItem.getFieldType() == SearchFieldType.evidence) {
            return SearchFieldType.general;
        }
        return fieldItem.getFieldType();
    }

    protected void addSearchFieldItems(List<SearchFieldItem> searchFieldItems) {
        if (this.searchFieldItems == null) {
            this.searchFieldItems = getSearchFieldItems();
        }
        this.searchFieldItems.addAll(searchFieldItems);
    }

    private boolean isSearchFieldItem(SearchFieldItem fieldItem) {
        return Objects.nonNull(fieldItem.getFieldType())
                && SearchFieldType.sort != fieldItem.getFieldType();
    }

    private boolean isSortFieldItem(SearchFieldItem fieldItem) {
        return SearchFieldType.sort.equals(fieldItem.getFieldType());
    }
}
