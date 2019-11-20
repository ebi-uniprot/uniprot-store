package org.uniprot.store.search.domain2;

import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.xdb.UniProtXDbTypes;

import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.uniprot.store.search.domain2.UniProtKBSearchFields.XREF_COUNT_PREFIX;

/**
 * Created 19/11/2019
 *
 * @author Edd
 */
class UniProtKBSearchFieldsTest {
    @Test
    void checkXrefCountFieldsIncludeAllDatabaseTypes() {
        assertThat(
                UniProtKBSearchFields.INSTANCE.getSearchFields().stream()
                        .filter(searchField -> searchField.getName().startsWith(XREF_COUNT_PREFIX))
                        .peek(System.out::println)
                        .collect(Collectors.toSet()),
                hasSize(UniProtXDbTypes.INSTANCE.getAllDBXRefTypes().size()));
    }
}
