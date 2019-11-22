package org.uniprot.store.search.domain2;

import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.xdb.UniProtXDbTypes;

import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.uniprot.store.search.domain2.UniProtKBSearchFields.XREF_COUNT_PREFIX;

/**
 * Created 19/11/2019
 *
 * @author Edd
 */
class UniProtKBSearchFieldsTest {

    @Test
    void canLoadAllFields() {
        List<String> allFieldNames =
                getFields(searchField -> !searchField.getName().startsWith(XREF_COUNT_PREFIX))
                        .stream()
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
                UniProtKBSearchFields.INSTANCE.getSorts(),
                containsInAnyOrder("accession_id", "annotation_score"));
    }

    @Test
    void checkXrefCountFieldsIncludeAllDatabaseTypes() {
        assertThat(
                UniProtKBSearchFields.INSTANCE.getSearchFields().stream()
                        .filter(searchField -> searchField.getName().startsWith(XREF_COUNT_PREFIX))
                        .peek(System.out::println)
                        .collect(Collectors.toSet()),
                hasSize(UniProtXDbTypes.INSTANCE.getAllDBXRefTypes().size()));
    }

    private Set<SearchField> getFields(Predicate<SearchField> predicate) {
        return UniProtKBSearchFields.INSTANCE.getSearchFields().stream()
                .filter(predicate)
                .peek(System.out::println)
                .collect(Collectors.toSet());
    }
}
