package org.uniprot.store.search.domain2;

import java.util.Optional;

/**
 * Created 12/11/2019
 *
 * @author Edd
 */
public interface SearchField {
    String getTerm();
    Optional<String> getSortTerm();
    Optional<String> getValidRegex();
    FieldType getType();
}
