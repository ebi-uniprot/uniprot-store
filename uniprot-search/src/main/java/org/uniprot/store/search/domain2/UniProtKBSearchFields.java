package org.uniprot.store.search.domain2;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.uniprot.core.cv.xdb.UniProtXDbTypes;
import org.uniprot.store.search.domain2.impl.SearchFieldImpl;

/**
 * Represents all accessible UniProtKB search fields, and provides access to them via the {@link
 * SearchFields} contract.
 *
 * <p>Created 12/11/2019
 *
 * @author Edd
 */
public enum UniProtKBSearchFields implements SearchFields {
    INSTANCE;

    private static final String FILENAME = "uniprot/search-fields.json";
    static final String XREF_COUNT_PREFIX = "xref_count_";
    private UniProtKBSearchFieldsLoader searchFieldsLoader;

    public static void main(String[] args) {
        UniProtKBSearchFields.INSTANCE.getSearchFields().stream().forEach(System.out::println);
        System.out.println("hello world");
    }

    UniProtKBSearchFields() {
        searchFieldsLoader = new UniProtKBSearchFieldsLoader(FILENAME);
    }

    @Override
    public Set<SearchField> getSearchFields() {
        return searchFieldsLoader.getSearchFields();
    }

    @Override
    public Set<SearchField> getSortFields() {
        return searchFieldsLoader.getSortFields();
    }

    private static class UniProtKBSearchFieldsLoader extends SearchFieldsLoader {

        UniProtKBSearchFieldsLoader(String fileName) {
            super(fileName);
        }

        @Override
        protected List<SearchField> extractSearchFields(List<SearchItem> allSearchItems) {
            List<SearchField> searchFields = super.extractSearchFields(allSearchItems);
            searchFields.addAll(getDbXrefsCountSearchFields());
            return searchFields;
        }

        private List<SearchFieldImpl> getDbXrefsCountSearchFields() {
            return UniProtXDbTypes.INSTANCE.getAllDBXRefTypes().stream()
                    .map(
                            db ->
                                    SearchFieldImpl.builder()
                                            .name(XREF_COUNT_PREFIX + db.getName().toLowerCase())
                                            .type(SearchFieldType.RANGE)
                                            .validRegex("^[0-9]+$")
                                            .build())
                    .collect(Collectors.toList());
        }
    }
}
