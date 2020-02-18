package org.uniprot.store.config.service;

import org.apache.commons.lang3.StringUtils;
import org.uniprot.store.config.model.ConfigFieldItem;
import org.uniprot.store.config.repository.ConfigFieldRepository;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public abstract class AbstractConfigFieldService implements ConfigFieldService {
    private ConfigFieldRepository repository;
    private List<ConfigFieldItem> searchFields;
    private List<ConfigFieldItem> sortFields;

    protected AbstractConfigFieldService(ConfigFieldRepository repository){
        this.repository = repository;
    }

    @Override
    public List<ConfigFieldItem> getAllFieldItems() {
        return this.repository.getFieldItems();
    }

    @Override
    public List<ConfigFieldItem> getSearchFieldItems() {
        if(this.searchFields == null) {
            this.searchFields = this.getAllFieldItems()
                    .stream()
                    .filter(this::isSearchField)
                    .collect(Collectors.toList());
        }

        return this.searchFields;
    }

    @Override
    public Optional<ConfigFieldItem> getSearchFieldItemByName(String fieldName) {
        return this.getSearchFieldItems()
                .stream()
                .filter(fi -> fieldName.equals(fi.getFieldName()))
                .findFirst();
    }

    @Override
    public boolean hasSearchFieldItem(String fieldName) {
        return this.getSearchFieldItemByName(fieldName).isPresent();
    }

    @Override
    public boolean isSearchFieldValueValid(String fieldName, String value) {
        Optional<ConfigFieldItem> optFieldItem = this.getSearchFieldItemByName(fieldName);
        if (optFieldItem.isPresent()) {
            String validRegex = optFieldItem.get().getValidRegex();
            if (StringUtils.isNotEmpty(validRegex)) {
                return value.matches(validRegex);
            } else {
                return true;
            }
        }

        throw new IllegalArgumentException("Search field does not exist: " + fieldName);
    }

    @Override
    public List<ConfigFieldItem> getSortFieldItems() {
        if(this.sortFields == null) {
            this.sortFields = this.getAllFieldItems()
                    .stream()
                    .filter(this::isSortField)
                    .collect(Collectors.toList());
        }

        return this.sortFields;
    }

    @Override
    public Optional<ConfigFieldItem> getSortFieldItemByName(String fieldName) {
        return this.getSortFieldItems()
                .stream()
                .filter(fi -> fieldName.equals(fi.getFieldName()))
                .findFirst();
    }

    @Override
    public boolean hasSortFieldItem(String fieldName) {
        return this.getSortFieldItemByName(fieldName).isPresent();
    }

    private boolean isSearchField(ConfigFieldItem fieldItem) {
        return Objects.nonNull(fieldItem.getFieldType())
                && ConfigFieldItem.FieldType.sort != fieldItem.getFieldType();
    }

    private boolean isSortField(ConfigFieldItem fieldItem) {
        return ConfigFieldItem.FieldType.sort.equals(fieldItem.getFieldType());
    }
}
