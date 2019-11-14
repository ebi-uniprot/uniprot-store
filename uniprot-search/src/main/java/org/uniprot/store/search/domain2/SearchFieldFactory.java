package org.uniprot.store.search.domain2;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.uniprot.store.search.domain2.impl.UniProtKBSearchItem;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Created 12/11/2019
 *
 * @author Edd
 */
public class SearchFieldFactory {
    // getSearchItems
    // getSearchFields
    // getTermFields
    // getRangeFields
    // getFieldsWithSorts

    private static final String FILENAME = "uniprot/search-fields.json";
    private List<SearchItem> searchItems = new ArrayList<>();

    private void init() {
        ObjectMapper objectMapper = getJsonMapper();
        try (InputStream is =
                SearchFieldFactory.class.getClassLoader().getResourceAsStream(FILENAME); ) {
            List<UniProtKBSearchItem> items =
                    objectMapper.readValue(is, new TypeReference<List<UniProtKBSearchItem>>() {});
            this.searchItems.addAll(items);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<SearchItem> getSearchItems() {
        return searchItems;
    }

    public static void main(String[] args) {
        SearchFieldFactory factory = new SearchFieldFactory();
        factory.init();
        factory.getSearchItems().stream().map(Object::toString).forEach(System.out::println);
    }

    public static ObjectMapper getJsonMapper() {
        final ObjectMapper objectMapper = new ObjectMapper();
        SimpleModule mod = new SimpleModule();
//        mod.addAbstractTypeMapping(EvidenceGroup.class, EvidenceGroupImpl.class);
//        mod.addAbstractTypeMapping(EvidenceItem.class, EvidenceItemImpl.class);
//        mod.addAbstractTypeMapping(FieldGroup.class, FieldGroupImpl.class);
//        mod.addAbstractTypeMapping(Field.class, FieldImpl.class);
        mod.addAbstractTypeMapping(SearchItem.class, UniProtKBSearchItem.class);
//        mod.addAbstractTypeMapping(Tuple.class, TupleImpl.class);
        objectMapper.registerModule(mod);
        return objectMapper;
    }
}
