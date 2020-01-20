package org.uniprot.store.search.domain2;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.uniprot.store.search.domain2.UniProtKBSearchFieldsLoader.XREF_COUNT_PREFIX;

/**
 * Other aspects of the loader are tested elsewhere. Therefore, this class just tests whether {@link
 * UniProtKBSearchFieldsLoader#extractSearchFields(List)} is correctly overridden. Created 20/01/20
 *
 * @author Edd
 */
class UniProtKBSearchFieldsLoaderTest {
    private static SearchFieldsLoader loader;

    @BeforeAll
    static void setUpClass() {
        loader = new UniProtKBSearchFieldsLoader("uniprot/search-fields.json");
    }

    @Test
    void generatedAtLeast1XrefCountField() {
        assertThat(
                loader.getSearchFields().stream()
                        .anyMatch(
                                searchField -> searchField.getName().startsWith(XREF_COUNT_PREFIX)),
                is(true));
    }
}
