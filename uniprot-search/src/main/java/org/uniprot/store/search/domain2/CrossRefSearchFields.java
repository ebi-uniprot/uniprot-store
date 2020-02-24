package org.uniprot.store.search.domain2;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.uniprot.store.config.searchfield.common.SearchFieldConfig;
import org.uniprot.store.config.searchfield.impl.CrossRefSearchFieldConfiguration;
import org.uniprot.store.config.searchfield.model.FieldItem;
import org.uniprot.store.config.searchfield.model.FieldType;
import org.uniprot.store.search.domain2.impl.SearchFieldImpl;

public class CrossRefSearchFields extends SearchFieldsLoader {
    private SearchFieldConfig configService;
    private Set<SearchField> searchFields;
    private Set<SearchField> sortFields;

    public CrossRefSearchFields() {
        this.configService = CrossRefSearchFieldConfiguration.getInstance();
    }

    @Override
    public Set<SearchField> getSearchFields() {
        if (this.searchFields == null) {

            this.searchFields =
                    this.configService.getAllFieldItems().stream()
                            .filter(this::isSearchField)
                            .map(SearchFieldImpl::from)
                            .collect(Collectors.toSet());
        }
        return this.searchFields;
    }

    @Override
    public SearchField getSortFieldFor(String field) {
        SearchField searchField = this.getField(field);
        SearchField sortField = null;
        if (searchField.getSortField().isPresent()) {
            String sortFieldId = searchField.getSortField().get().getName();
            if (Objects.nonNull(this.configService.getFieldItemById(sortFieldId))) {
                sortField = SearchFieldImpl.from(this.configService.getFieldItemById(sortFieldId));
            }
        }
        if (sortField == null) {
            throw new IllegalArgumentException(
                    "Field '" + field + "' does not have an associated sort field.");
        }

        return sortField;
    }

    @Override
    public Set<SearchField> getSortFields() {
        if (this.sortFields == null) {
            this.sortFields =
                    this.configService.getAllFieldItems().stream()
                            .filter(this::isSortField)
                            .map(SearchFieldImpl::from)
                            .collect(Collectors.toSet());
        }
        return this.sortFields;
    }

    private boolean isSearchField(FieldItem fieldItem) {
        return Objects.nonNull(fieldItem.getFieldType())
                && FieldType.sort != fieldItem.getFieldType();
    }

    private boolean isSortField(FieldItem fieldItem) {
        return FieldType.sort.equals(fieldItem.getFieldType());
    }
}
