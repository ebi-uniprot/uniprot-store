package org.uniprot.store.search.domain2;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.uniprot.core.util.Utils;
import org.uniprot.store.search.domain2.impl.SearchItemImpl;

import java.util.ArrayList;
import java.util.List;

/**
 * This class is responsible for loading {@link SearchField} instances from a given JSON source.
 *
 * <p>Created 20/11/19
 *
 * @author Edd
 */
public class SearchItemsLoader implements SearchItems {
    private List<SearchItem> searchItems = new ArrayList<>();

    SearchItemsLoader(String fileName) {
        init(fileName);
    }

    private void init(String fileName) {
        ObjectMapper mapper = getJsonMapper();
        JavaType type =
                mapper.getTypeFactory().constructCollectionType(List.class, SearchItem.class);
        List<SearchItem> allItems = JsonLoader.loadItems(fileName, mapper, type);

        // search items (used by front-end)
        allItems.stream()
                .filter(item -> Utils.notNullOrEmpty(item.getLabel()))
                .forEach(searchItems::add);
    }

    @Override
    public List<SearchItem> getSearchItems() {
        return searchItems;
    }

    private static ObjectMapper getJsonMapper() {
        final ObjectMapper objectMapper = new ObjectMapper();
        SimpleModule mod = new SimpleModule();
        mod.addAbstractTypeMapping(SearchItem.class, SearchItemImpl.class);
        objectMapper.registerModule(mod);
        return objectMapper;
    }
}
