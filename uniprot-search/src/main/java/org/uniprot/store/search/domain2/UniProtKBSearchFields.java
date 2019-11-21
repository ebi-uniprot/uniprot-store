package org.uniprot.store.search.domain2;

import org.uniprot.core.cv.xdb.UniProtXDbTypes;
import org.uniprot.store.search.domain2.impl.SearchFieldImpl;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Represents all accessible UniProtKB search fields, and provides access to them via both
 * contracts, {@link SearchItems} and {@link SearchFields}.
 *
 * <p>Created 12/11/2019
 *
 * @author Edd
 */
public enum UniProtKBSearchFields implements SearchItems, SearchFields {
    INSTANCE;

    private static final String FILENAME = "uniprot/search-fields.json";
    static final String XREF_COUNT_PREFIX = "xref_count_";
    private UniProtKBSearchFieldsLoader searchFieldsLoader;

    UniProtKBSearchFields() {
        searchFieldsLoader = new UniProtKBSearchFieldsLoader(FILENAME);
    }

    @Override
    public List<SearchItem> getSearchItems() {
        return searchFieldsLoader.getSearchItems();
    }

    @Override
    public Set<SearchField> getSearchFields() {
        return searchFieldsLoader.getSearchFields();
    }

    @Override
    public Set<SearchField> getGeneralFields() {
        return searchFieldsLoader.getGeneralFields();
    }

    @Override
    public Set<SearchField> getRangeFields() {
        return searchFieldsLoader.getRangeFields();
    }

    @Override
    public Set<String> getSorts() {
        return searchFieldsLoader.getSorts();
    }

    public static void main(String[] args) {
        UniProtKBSearchFields.INSTANCE.getSearchItems().stream()
                .map(Object::toString)
                .forEach(System.out::println);
    }

    private static class UniProtKBSearchFieldsLoader extends SearchFieldsLoader {

        UniProtKBSearchFieldsLoader(String fileName) {
            super(fileName);
        }

        @Override
        protected Set<SearchField> extractSearchFields(List<SearchItem> allSearchItems) {
            Set<SearchField> searchFields = super.extractSearchFields(allSearchItems);
            searchFields.addAll(getDbXrefsCountSearchFields());
            return searchFields;
        }

        private List<SearchFieldImpl> getDbXrefsCountSearchFields() {
            return UniProtXDbTypes.INSTANCE.getAllDBXRefTypes().stream()
                    .map(
                            db ->
                                    SearchFieldImpl.builder()
                                            .name(XREF_COUNT_PREFIX + db.getName().toLowerCase())
                                            .type(SearchFieldType.GENERAL)
                                            .build())
                    .collect(Collectors.toList());
        }
    }
}
