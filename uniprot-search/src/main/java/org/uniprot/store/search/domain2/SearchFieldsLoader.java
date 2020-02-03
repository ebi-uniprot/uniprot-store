package org.uniprot.store.search.domain2;

import java.util.*;
import java.util.stream.Collectors;

import org.uniprot.core.util.Utils;
import org.uniprot.store.search.domain2.impl.SearchFieldImpl;
import org.uniprot.store.search.domain2.impl.SearchItemImpl;
import org.uniprot.store.search.field.SearchFields;

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
public class SearchFieldsLoader implements SearchFields {
    private Set<SearchField> searchFields = new HashSet<>();
    private Set<SearchField> sortFields;

    public SearchFieldsLoader(String fileName) {
        init(fileName);
    }

    private void init(String fileName) {
        ObjectMapper mapper = getJsonMapper();
        JavaType type =
                mapper.getTypeFactory().constructCollectionType(List.class, SearchItem.class);
        List<SearchItem> allItems = JsonLoader.loadItems(fileName, mapper, type);

        // all search fields used in application code
        List<SearchField> allSearchFields = extractSearchFields(allItems);
        SearchFieldsValidator.validate(allSearchFields);
        searchFields.addAll(allSearchFields);

        // sorts
        sortFields =
                searchFields.stream()
                        .map(SearchField::getSortField)
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .collect(Collectors.toSet());
    }

    protected List<SearchField> extractSearchFields(List<SearchItem> allSearchItems) {
        return allSearchItems.stream()
                .map(this::searchItemToSearchField)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
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
                SearchFieldImpl.SearchFieldImplBuilder fieldBuilder = SearchFieldImpl.builder();

                if (Utils.notNullOrEmpty(searchItem.getSortField())) {
                    SearchFieldImpl sortField =
                            SearchFieldImpl.builder()
                                    .name(searchItem.getSortField())
                                    .type(SearchFieldType.GENERAL)
                                    .validRegex(searchItem.getFieldValidRegex())
                                    .build();
                    fieldBuilder.sortField(sortField);
                }

                fields.add(
                        fieldBuilder
                                .name(searchItem.getField())
                                .type(SearchFieldType.GENERAL)
                                .validRegex(searchItem.getFieldValidRegex())
                                .build());
            }

            // range
            if (Utils.notNullOrEmpty(searchItem.getRangeField())) {
                SearchFieldImpl.SearchFieldImplBuilder fieldBuilder = SearchFieldImpl.builder();
                if (searchItem.getField() == null
                        && Utils.notNullOrEmpty(searchItem.getSortField())) {
                    SearchFieldImpl sortField =
                            SearchFieldImpl.builder()
                                    .name(searchItem.getSortField())
                                    .type(SearchFieldType.RANGE)
                                    .validRegex(searchItem.getFieldValidRegex())
                                    .build();
                    fieldBuilder.sortField(sortField);
                }

                fields.add(
                        fieldBuilder
                                .name(searchItem.getRangeField())
                                .type(SearchFieldType.RANGE)
                                .validRegex(searchItem.getFieldValidRegex())
                                .build());
            }

            // evidence
            if (Utils.notNullOrEmpty(searchItem.getEvidenceField())) {
                SearchFieldImpl field =
                        SearchFieldImpl.builder()
                                .name(searchItem.getEvidenceField())
                                .type(SearchFieldType.GENERAL)
                                .build();

                if (!fields.contains(field)) {
                    fields.add(field);
                }
            }

            // id
            if (Utils.notNullOrEmpty(searchItem.getIdField())) {
                SearchFieldImpl field =
                        SearchFieldImpl.builder()
                                .name(searchItem.getIdField())
                                .type(SearchFieldType.GENERAL)
                                .validRegex(searchItem.getIdValidRegex())
                                .build();

                if (!fields.contains(field)) {
                    fields.add(field);
                }
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
    public Set<SearchField> getSortFields() {
        return sortFields;
    }

    private static ObjectMapper getJsonMapper() {
        final ObjectMapper objectMapper = new ObjectMapper();
        SimpleModule mod = new SimpleModule();
        mod.addAbstractTypeMapping(SearchItem.class, SearchItemImpl.class);
        objectMapper.registerModule(mod);
        return objectMapper;
    }
}
