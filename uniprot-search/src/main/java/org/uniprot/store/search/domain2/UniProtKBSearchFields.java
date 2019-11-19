package org.uniprot.store.search.domain2;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.uniprot.core.cv.xdb.UniProtXDbTypes;
import org.uniprot.core.util.Utils;
import org.uniprot.store.search.domain2.impl.SearchFieldImpl;
import org.uniprot.store.search.domain2.impl.SearchItemImpl;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;

/**
 * Created 12/11/2019
 *
 * @author Edd
 */
public enum UniProtKBSearchFields implements SearchItems, SearchFields {
    INSTANCE;

    private static final String FILENAME = "uniprot/search-fields.json";
    static final String XREF_COUNT_PREFIX = "xref_count_";
    private List<SearchItem> searchItems = new ArrayList<>();
    private Set<SearchField> searchFields = new HashSet<>();
    private Map<SearchFieldType, Set<SearchField>> fieldsByType;
    private Set<String> sortFieldNames;

    private void init() {
        ObjectMapper mapper = getJsonMapper();
        JavaType type =
                mapper.getTypeFactory().constructCollectionType(List.class, SearchItem.class);
        List<SearchItem> allItems = JsonLoader.loadItems(FILENAME, mapper, type);

        // search items (used by front-end)
        allItems.stream()
                .filter(item -> Utils.notNullOrEmpty(item.getLabel()))
                .forEach(searchItems::add);

        // all search fields used in application code
        allItems.stream()
                .map(this::searchItemToSearchField)
                .distinct()
                .forEach(searchFields::addAll);
        addDbXrefs();
        verifyNoDuplicateFields();

        // record fields by type
        fieldsByType =
                searchFields.stream()
                        .collect(
                                groupingBy(
                                        SearchField::getType,
                                        Collectors.mapping(
                                                Function.identity(), Collectors.toSet())));

        // sorts
        sortFieldNames =
                allItems.stream()
                        .filter(field -> Utils.notNullOrEmpty(field.getSortField()))
                        .map(SearchItem::getSortField)
                        .collect(Collectors.toSet());
    }

    private void verifyNoDuplicateFields() {
        List<String> fieldNames = new ArrayList<>();
        for (SearchField searchField : searchFields) {
            fieldNames.add(searchField.getName());
            searchField
                    .getSortName()
                    .filter(sortName -> !sortName.equals(searchField.getName()))
                    .ifPresent(fieldNames::add);
        }

        Set<String> allItems = new HashSet<>();
        Set<String> duplicates =
                fieldNames.stream().filter(name -> !allItems.add(name)).collect(Collectors.toSet());
        if (!duplicates.isEmpty()) {
            throw new IllegalStateException(
                    "Duplicate field names found: " + Arrays.toString(duplicates.toArray()));
        }
    }

    private void addDbXrefs() {
        List<SearchFieldImpl> dbXrefCountFields =
                UniProtXDbTypes.INSTANCE.getAllDBXRefTypes().stream()
                        .map(
                                db ->
                                        SearchFieldImpl.builder()
                                                .name(
                                                        XREF_COUNT_PREFIX
                                                                + db.getName().toLowerCase())
                                                .type(SearchFieldType.GENERAL)
                                                .build())
                        .collect(Collectors.toList());

        searchFields.addAll(dbXrefCountFields);
    }

    private List<SearchField> searchItemToSearchField(SearchItem searchItem) {
        List<SearchField> fields = new ArrayList<>();

        searchItemToSearchField(searchItem, fields);

        return fields;
    }

    private void searchItemToSearchField(SearchItem searchItem, List<SearchField> fields) {
        if (!searchItem.getItemType().equals("group")
                && !searchItem.getItemType().equals("groupDisplay")) {
            if (Utils.notNullOrEmpty(searchItem.getRangeField())) {
                // range
                fields.add(
                        SearchFieldImpl.builder()
                                .name(searchItem.getRangeField())
                                .type(SearchFieldType.RANGE)
                                .validRegex(searchItem.getIdValidRegex())
                                .build());
            }

            if (Utils.notNullOrEmpty(searchItem.getField())){
                // standard
                fields.add(
                        SearchFieldImpl.builder()
                                .name(searchItem.getField())
                                .sortName(searchItem.getSortField())
                                .type(SearchFieldType.GENERAL)
                                .validRegex(searchItem.getFieldValidRegex())
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

    UniProtKBSearchFields() {
        init();
    }

    public List<SearchItem> getSearchItems() {
        return searchItems;
    }

    @Override
    public Set<SearchField> getSearchFields() {
        return searchFields;
    }

    @Override
    public Set<SearchField> getGeneralFields() {
        return fieldsByType.get(SearchFieldType.GENERAL);
    }

    @Override
    public Set<SearchField> getRangeFields() {
        return fieldsByType.get(SearchFieldType.RANGE);
    }

    @Override
    public Set<String> getSorts() {
        return sortFieldNames;
    }

    public static void main(String[] args) {
        UniProtKBSearchFields.INSTANCE.getSearchItems().stream()
                .map(Object::toString)
                .forEach(System.out::println);
    }

    private static ObjectMapper getJsonMapper() {
        final ObjectMapper objectMapper = new ObjectMapper();
        SimpleModule mod = new SimpleModule();
        mod.addAbstractTypeMapping(SearchItem.class, SearchItemImpl.class);
        objectMapper.registerModule(mod);
        return objectMapper;
    }
}
