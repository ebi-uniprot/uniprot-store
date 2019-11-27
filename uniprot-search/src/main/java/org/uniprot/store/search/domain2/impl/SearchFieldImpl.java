package org.uniprot.store.search.domain2.impl;

import java.util.Optional;

import lombok.Builder;
import lombok.Data;

import org.uniprot.store.search.domain2.SearchField;
import org.uniprot.store.search.domain2.SearchFieldType;

/**
 * Created 14/11/19
 *
 * @author Edd
 */
@Builder
@Data
public class SearchFieldImpl implements SearchField {
    private String name;
    private SearchFieldType type;
    private SearchField sortField;
    private String validRegex;

    @Override
    public Optional<SearchField> getSortField() {
        return Optional.ofNullable(sortField);
    }

    @Override
    public Optional<String> getValidRegex() {
        return Optional.ofNullable(validRegex);
    }
}
