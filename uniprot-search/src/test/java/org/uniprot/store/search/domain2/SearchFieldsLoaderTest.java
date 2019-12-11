package org.uniprot.store.search.domain2;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

/**
 * Created 20/11/19
 *
 * @author Edd
 */
class SearchFieldsLoaderTest {
    private static final String FILENAME = "uniprot/search-fields.json";
    private static SearchFieldsLoader fieldsLoader;

    @BeforeAll
    static void setUp() {
        fieldsLoader = new SearchFieldsLoader(FILENAME);
    }

    @Test
    void canLoadAllFields() {
        List<String> allFieldNames =
                fieldsLoader.getSearchFields().stream()
                        .map(SearchField::getName)
                        .collect(Collectors.toList());

        assertThat(
                allFieldNames,
                containsInAnyOrder(
                        "annotation_score",
                        "mnemonic_default",
                        "accession",
                        "ec",
                        "cc_cofactor_chebi",
                        "cc_cofactor_note",
                        "ccev_cofactor_chebi",
                        "ccev_cofactor_note",
                        "pretend_range_field"));
    }

    @Test
    void checkSortFields() {
        assertThat(
                fieldsLoader.getSortFields().stream()
                        .map(SearchField::getName)
                        .collect(Collectors.toList()),
                containsInAnyOrder("accession_id", "annotation_score"));
    }
}
