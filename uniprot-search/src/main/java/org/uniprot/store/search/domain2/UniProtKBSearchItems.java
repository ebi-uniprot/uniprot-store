package org.uniprot.store.search.domain2;

import java.util.List;

/**
 * Represents all accessible UniProtKB search items, and provides access to them via the {@link
 * SearchItems} contract.
 *
 * <p>Created 22/11/2019
 *
 * @author Edd
 */
public enum UniProtKBSearchItems implements SearchItems {
    INSTANCE;

    private static final String FILENAME = "uniprot/search-fields.json";
    private final SearchItems searchItems;

    UniProtKBSearchItems() {
        searchItems = new SearchItemsLoader(FILENAME);
    }

    @Override
    public List<SearchItem> getSearchItems() {
        return searchItems.getSearchItems();
    }
}
