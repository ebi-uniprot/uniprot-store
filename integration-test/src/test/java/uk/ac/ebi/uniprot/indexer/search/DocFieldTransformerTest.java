package uk.ac.ebi.uniprot.indexer.search;

import org.apache.solr.client.solrj.beans.Field;
import org.junit.Before;
import org.junit.Test;
import uk.ac.ebi.uniprot.search.document.Document;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static uk.ac.ebi.uniprot.indexer.search.DocFieldTransformer.fieldTransformer;

/**
 * Created 02/07/18
 *
 * @author Edd
 */
public class DocFieldTransformerTest {
    private static final String INIT_NAME = "Bill";
    private static final int INIT_AGE = 20;
    private static final List<String> INIT_FAVE_COLOURS = asList("red", "yellow");
    private final MyDocument myDocument = new MyDocument();

    @Before
    public void setUp() {
        myDocument.nameField = INIT_NAME;
        myDocument.ageField = INIT_AGE;
        myDocument.favouriteColoursField = INIT_FAVE_COLOURS;
    }

    @Test
    public void canTransformStringField() {
        String newName = "Sam";
        DocFieldTransformer transformer = fieldTransformer("name", newName);
        assertThat(myDocument.nameField, is(INIT_NAME));

        transformer.accept(myDocument);

        assertThat(myDocument.nameField, is(newName));
    }

    @Test
    public void canTransformIntField() {
        int newAge = INIT_AGE + 5;
        DocFieldTransformer transformer = fieldTransformer("age", newAge);
        assertThat(myDocument.ageField, is(INIT_AGE));

        transformer.accept(myDocument);

        assertThat(myDocument.ageField, is(newAge));
    }

    @Test
    public void canTransformListField() {
        List<String> newFaveColours = new ArrayList<>(INIT_FAVE_COLOURS);
        newFaveColours.add("Green");

        DocFieldTransformer transformer = fieldTransformer("favouriteColours", newFaveColours);
        assertThat(myDocument.favouriteColoursField, is(INIT_FAVE_COLOURS));

        transformer.accept(myDocument);

        assertThat(myDocument.favouriteColoursField, is(newFaveColours));
    }

    @Test
    public void invalidFieldCausesException() {
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