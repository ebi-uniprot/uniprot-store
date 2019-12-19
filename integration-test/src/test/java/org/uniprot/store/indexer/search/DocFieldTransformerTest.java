package org.uniprot.store.indexer.search;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.uniprot.store.indexer.search.DocFieldTransformer.fieldTransformer;

import java.util.ArrayList;
import java.util.List;

import org.apache.solr.client.solrj.beans.Field;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.uniprot.store.search.document.Document;

/**
 * Created 02/07/18
 *
 * @author Edd
 */
class DocFieldTransformerTest {
    private static final String INIT_NAME = "Bill";
    private static final int INIT_AGE = 20;
    private static final List<String> INIT_FAVE_COLOURS = asList("red", "yellow");
    private final MyDocument myDocument = new MyDocument();

    @BeforeEach
    void setUp() {
        myDocument.nameField = INIT_NAME;
        myDocument.ageField = INIT_AGE;
        myDocument.favouriteColoursField = INIT_FAVE_COLOURS;
    }

    @Test
    void canTransformStringField() {
        String newName = "Sam";
        DocFieldTransformer transformer = fieldTransformer("name", newName);
        assertThat(myDocument.nameField, is(INIT_NAME));

        transformer.accept(myDocument);

        assertThat(myDocument.nameField, is(newName));
    }

    @Test
    void canTransformIntField() {
        int newAge = INIT_AGE + 5;
        DocFieldTransformer transformer = fieldTransformer("age", newAge);
        assertThat(myDocument.ageField, is(INIT_AGE));

        transformer.accept(myDocument);

        assertThat(myDocument.ageField, is(newAge));
    }

    @Test
    void canTransformListField() {
        List<String> newFaveColours = new ArrayList<>(INIT_FAVE_COLOURS);
        newFaveColours.add("Green");

        DocFieldTransformer transformer = fieldTransformer("favouriteColours", newFaveColours);
        assertThat(myDocument.favouriteColoursField, is(INIT_FAVE_COLOURS));

        transformer.accept(myDocument);

        assertThat(myDocument.favouriteColoursField, is(newFaveColours));
    }

    @Test
    void invalidFieldCausesException() {
        DocFieldTransformer transformer = fieldTransformer("height", 10);

        assertThrows(IllegalStateException.class, () -> transformer.accept(myDocument));
    }

    private static class MyDocument implements Document {
        @Field("name")
        String nameField;

        @Field("favouriteColours")
        List<String> favouriteColoursField;

        @Field("age")
        int ageField;

        @Override
        public String getDocumentId() {
            return nameField;
        }
    }
}
