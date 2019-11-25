package org.uniprot.store.search.domain2;

import java.util.List;

/**
 * Represents a container of {@link SearchItem} instances, providing utility methods to ease their
 * access.
 *
 * Created 15/11/19
 *
 * @author Edd
 */
public interface SearchItems {
    List<SearchItem> getSearchItems();
}

