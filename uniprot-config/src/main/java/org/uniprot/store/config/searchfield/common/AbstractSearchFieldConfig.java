package org.uniprot.store.config.searchfield.common;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.StringUtils;
import org.uniprot.store.config.UniProtDataType;
import org.uniprot.store.config.common.JsonLoader;
import org.uniprot.store.config.schema.SchemaValidator;
import org.uniprot.store.config.searchfield.model.SearchFieldItem;
import org.uniprot.store.config.searchfield.model.SearchFieldType;
import org.uniprot.store.config.searchfield.schema.SearchFieldDataValidator;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

@Slf4j
public abstract class AbstractSearchFieldConfig implements SearchFieldConfig {
    public static final String SCHEMA_FILE = "schema/search-fields-schema.json";
    protected UniProtDataType dataType;

    private List<SearchFieldItem> fieldItems;
    private List<SearchFieldItem> searchFieldItems;
    private List<SearchFieldItem> sortFieldItems;

    protected AbstractSearchFieldConfig(
            UniProtDataType dataType, String schemaFile, String configFile) {
        this.dataType = dataType;
        SchemaValidator.validate(schemaFile, configFile);
        init(configFile);
        new SearchFieldDataValidator().validateContent(this.fieldItems);
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

    @Override
    public Set<String> getSearchFieldNames() {
        return getSearchFieldItems().stream()
                .map(SearchFieldItem::getFieldName)
                .collect(Collectors.toSet());
    }

    public SearchFieldItem getSearchFieldItemByName(String fieldName) {
        return this.getSearchFieldItems().stream()
                .filter(fi -> fieldName.equalsIgnoreCase(fi.getFieldName()))
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
        if (fieldItem.getFieldType() == SearchFieldType.EVIDENCE) {
            return SearchFieldType.GENERAL;
        }
        return fieldItem.getFieldType();
    }

    protected abstract Collection<SearchFieldItem> dynamicallyLoadFields();

    private void init(String configFile) {
        ObjectMapper mapper = new ObjectMapper();
        JavaType type =
                mapper.getTypeFactory().constructCollectionType(List.class, SearchFieldItem.class);

        this.fieldItems = JsonLoader.loadItems(configFile, mapper, type);
        this.fieldItems.addAll(dynamicallyLoadFields());
    }

    private boolean isSearchFieldItem(SearchFieldItem fieldItem) {
        return Objects.nonNull(fieldItem.getFieldType())
                && SearchFieldType.SORT != fieldItem.getFieldType();
    }

    private boolean isSortFieldItem(SearchFieldItem fieldItem) {
        return SearchFieldType.SORT.equals(fieldItem.getFieldType());
    }
}
