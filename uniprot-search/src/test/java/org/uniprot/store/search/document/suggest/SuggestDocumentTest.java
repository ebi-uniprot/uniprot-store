package org.uniprot.store.search.document.suggest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;
import static org.uniprot.store.search.document.suggest.SuggestDocument.DEFAULT_IMPORTANCE;

import org.junit.jupiter.api.Test;

/**
 * Created 25/05/19
 *
 * @author Edd
 */
class SuggestDocumentTest {

    public static final String SOME_VALUE = "SomeValue";

    @Test
    void canBuildSuggestDocumentWithDefaultImportanceBuilder() {
        String id = "id";
        SuggestDocument suggestDocument = SuggestDocument.builder().id(id).build();
        assertThat(suggestDocument.id, is(id));
        assertThat(suggestDocument.importance, is(DEFAULT_IMPORTANCE));
    }

    @Test
    void addingNullToAltValuesIsIgnored() {
        SuggestDocument suggestDocument = SuggestDocument.builder().altValue(null).build();
        assertThat(suggestDocument.altValues, is(empty()));
    }

    @Test
    void addingEmptyToAltValuesIsIgnored() {
        SuggestDocument suggestDocument = SuggestDocument.builder().altValue("").build();
        assertThat(suggestDocument.altValues, is(empty()));
    }

    @Test
    void addingToAltValuesIsIgnored() {
        SuggestDocument suggestDocument =
                SuggestDocument.builder().altValue(null).altValue("").altValue(SOME_VALUE).build();
        assertThat(suggestDocument.altValues.size(), is(1));
        assertThat(suggestDocument.altValues.get(0), is(SOME_VALUE));
    }
}
