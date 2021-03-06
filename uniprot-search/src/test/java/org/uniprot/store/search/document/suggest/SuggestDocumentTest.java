package org.uniprot.store.search.document.suggest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.uniprot.store.search.document.suggest.SuggestDocument.DEFAULT_IMPORTANCE;

import org.junit.jupiter.api.Test;

/**
 * Created 25/05/19
 *
 * @author Edd
 */
class SuggestDocumentTest {
    @Test
    void canBuildSuggestDocumentWithDefaultImportanceBuilder() {
        String id = "id";
        SuggestDocument suggestDocument = SuggestDocument.builder().id(id).build();
        assertThat(suggestDocument.id, is(id));
        assertThat(suggestDocument.importance, is(DEFAULT_IMPORTANCE));
    }
}
