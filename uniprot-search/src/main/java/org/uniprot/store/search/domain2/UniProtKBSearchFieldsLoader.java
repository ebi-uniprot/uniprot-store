package org.uniprot.store.search.domain2;

import java.util.List;
import java.util.stream.Collectors;

import org.uniprot.core.cv.xdb.UniProtXDbTypes;
import org.uniprot.store.search.domain2.impl.SearchFieldImpl;

/**
 * UniProtKB search fields originate from both the {@code search-fields.json} file, and {@link
 * UniProtXDbTypes}. The latter is used to define a specific count field for all possible database
 * cross-references.
 *
 * <p>Created 20/01/20
 *
 * @author Edd
 */
public class UniProtKBSearchFieldsLoader extends SearchFieldsLoader {
    static final String XREF_COUNT_PREFIX = "xref_count_";

    public UniProtKBSearchFieldsLoader(String fileName) {
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
                                        .build())
                .collect(Collectors.toList());
    }
}
