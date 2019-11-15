package org.uniprot.store.search.domain2.impl;

import lombok.Builder;
import lombok.Data;
import org.uniprot.store.search.domain2.SearchFieldType;
import org.uniprot.store.search.domain2.SearchField;

import java.util.Optional;

/**
 * Created 14/11/19
 *
 * @author Edd
 */
@Builder
@Data
public class SearchFieldImpl implements SearchField {
    private String term;
    private SearchFieldType type;
    private String sortTerm;
    private String validRegex;

    @Override
    public Optional<String> getSortTerm() {
        return Optional.ofNullable(sortTerm);
    }

    @Override
    public Optional<String> getValidRegex() {
        return Optional.ofNullable(validRegex);
    }
}
