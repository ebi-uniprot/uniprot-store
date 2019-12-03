package org.uniprot.store.search.domain2;

import java.util.*;
import java.util.stream.Collectors;

import org.uniprot.core.util.Utils;
import org.uniprot.store.search.domain2.impl.SearchFieldImpl;
import org.uniprot.store.search.domain2.impl.SearchItemImpl;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

/**
 * This class is responsible for loading {@link SearchField} instances from a given JSON source.
 *
 * <p>Created 20/11/19
 *
 * @author Edd
 */
public class SearchFieldsLoader implements SearchItems, SearchFields {
    private List<SearchItem> searchItems = new ArrayList<>();
    private Set<SearchField> searchFields = new HashSet<>();
    private Set<String> sortFieldNames;

    SearchFieldsLoader(String fileName) {
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

        // all search fields used in application code
        searchFields.addAll(extractSearchFields(allItems));
        SearchFieldsValidator.validate(searchFields);

        // sorts
        sortFieldNames =
                allItems.stream()
                        .filter(field -> Utils.notNullOrEmpty(field.getSortField()))
                        .map(SearchItem::getSortField)
                        .collect(Collectors.toSet());
    }

    protected Set<SearchField> extractSearchFields(List<SearchItem> allSearchItems) {
        return allSearchItems.stream()
                .map(this::searchItemToSearchField)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
    }

    private List<SearchField> searchItemToSearchField(SearchItem searchItem) {
        List<SearchField> fields = new ArrayList<>();

        searchItemToSearchField(searchItem, fields);

        return fields;
    }

    private void searchItemToSearchField(SearchItem searchItem, List<SearchField> fields) {
        if (!searchItem.getItemType().equals("group")
                && !searchItem.getItemType().equals("groupDisplay")) {

            // general
            if (Utils.notNullOrEmpty(searchItem.getField())) {
                fields.add(
                        SearchFieldImpl.builder()
                                .name(searchItem.getField())
                                .sortName(searchItem.getSortField())
                                .type(SearchFieldType.GENERAL)
                                .validRegex(searchItem.getFieldValidRegex())
                                .build());
            }

            // range
            if (Utils.notNullOrEmpty(searchItem.getRangeField())) {
                fields.add(
                        SearchFieldImpl.builder()
                                .name(searchItem.getRangeField())
                                .type(SearchFieldType.RANGE)
                                .validRegex(searchItem.getIdValidRegex())
                                .build());
            }

            // evidence
            if (Utils.notNullOrEmpty(searchItem.getEvidenceField())) {
                fields.add(
                        SearchFieldImpl.builder()
                                .name(searchItem.getEvidenceField())
                                .type(SearchFieldType.GENERAL)
                                .build());
            }

            // id
            if (Utils.notNullOrEmpty(searchItem.getIdField())) {
                fields.add(
                        SearchFieldImpl.builder()
                                .name(searchItem.getIdField())
                                .type(SearchFieldType.GENERAL)
                                .validRegex(searchItem.getIdValidRegex())
                                .build());
            }
        }

        if (Utils.notNullOrEmpty(searchItem.getItems())) {
            searchItem.getItems().forEach(field -> searchItemToSearchField(field, fields));
        }
    }

    @Override
    public Set<SearchField> getSearchFields() {
        return searchFields;
    }

    @Override
    public Set<String> getSorts() {
        return sortFieldNames;
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
