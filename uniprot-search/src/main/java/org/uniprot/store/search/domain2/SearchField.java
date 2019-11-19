package org.uniprot.store.search.domain2;

import java.util.Optional;

/**
 * Represents fields used by the search engine.
 *
 * Created 12/11/2019
 *
 * @author Edd
 */
public interface SearchField {
    String getName();

    SearchFieldType getType();

    default Optional<String> getSortName() {
        return Optional.empty();
    }

    default Optional<String> getValidRegex() {
        return Optional.empty();
    }
}
