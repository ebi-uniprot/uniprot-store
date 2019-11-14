package org.uniprot.store.search.domain2;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.uniprot.core.util.Utils;
import org.uniprot.store.search.domain2.impl.SearchFieldImpl;
import org.uniprot.store.search.domain2.impl.UniProtKBSearchItem;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;

/**
 * Created 12/11/2019
 *
 * @author Edd
 */
public enum UniProtKBSearchFields implements SearchFields {
    INSTANCE;

    private static final String FILENAME = "uniprot/search-fields.json";
    private List<SearchItem> searchItems = new ArrayList<>();
    private List<SearchField> searchFields = new ArrayList<>();
    private Map<FieldType, List<SearchField>> fieldsByType;
    private List<String> sortFieldNames;

    private void init() {
        ObjectMapper objectMapper = getJsonMapper();
        try (InputStream is =
                UniProtKBSearchFields.class.getClassLoader().getResourceAsStream(FILENAME); ) {
            List<SearchItem> allItems =
                    objectMapper.readValue(is, new TypeReference<List<UniProtKBSearchItem>>() {});

            // search terms (used by front-end)
            allItems.stream()
                    .filter(item -> Utils.notNullOrEmpty(item.getLabel()))
                    .forEach(searchItems::add);

            // all search fields used in application code
            allItems.stream()
                    .map(this::searchItemToSearchField)
                    .distinct()
                    .forEach(searchFields::addAll);

            // record fields by type
            fieldsByType = searchFields.stream().collect(groupingBy(SearchField::getType));

            // sorts
            sortFieldNames =
                    allItems.stream()
                            .filter(field -> Utils.notNullOrEmpty(field.getSortTerm()))
                            .map(SearchItem::getSortTerm)
                            .collect(Collectors.toList());

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private List<SearchField> searchItemToSearchField(SearchItem searchItem) {
        List<SearchField> fields = new ArrayList<>();

        searchItemToSearchField(searchItem, fields);

        return fields;
    }

    private void searchItemToSearchField(SearchItem searchItem, List<SearchField> fields) {
        if (Utils.notNullOrEmpty(searchItem.getRangeTerm())) {
            // range term
            fields.add(
                    SearchFieldImpl.builder()
                            .term(searchItem.getRangeTerm())
                            .sortTerm(searchItem.getSortTerm())
                            .type(FieldType.RANGE)
                            .validRegex(searchItem.getTermValidRegex())
                            .build());
        } else {
            // standard term
            fields.add(
                    SearchFieldImpl.builder()
                            .term(searchItem.getTerm())
                            .sortTerm(searchItem.getSortTerm())
                            .type(FieldType.TERM)
                            .validRegex(searchItem.getTermValidRegex())
                            .build());
        }

        // id term
        if (Utils.notNullOrEmpty(searchItem.getIdTerm())) {
            fields.add(
                    SearchFieldImpl.builder()
                            .term(searchItem.getIdTerm())
                            .type(FieldType.TERM)
                            .validRegex(searchItem.getIdValidRegex())
                            .build());
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
    public List<SearchField> getSearchFields() {
        return searchFields;
    }

    @Override
    public List<SearchField> getTermFields() {
        return fieldsByType.get(FieldType.TERM);
    }

    @Override
    public List<SearchField> getRangeFields() {
        return fieldsByType.get(FieldType.RANGE);
    }

    @Override
    public List<String> getSorts() {
        return sortFieldNames;
    }

    public static void main(String[] args) {
        UniProtKBSearchFields.INSTANCE.getSorts().stream()
                .map(Object::toString)
                .forEach(System.out::println);
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
