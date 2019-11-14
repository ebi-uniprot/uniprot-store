package org.uniprot.store.search.domain2.impl;

import lombok.Builder;
import lombok.Data;
import org.uniprot.store.search.domain2.FieldType;
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
    private FieldType type;
    private String sortTerm;
    private String validRegex;

    public Optional<String> getSortTerm() {
        return Optional.ofNullable(sortTerm);
    }

    public Optional<String> getValidRegex() {
        return Optional.ofNullable(validRegex);
    }
}
