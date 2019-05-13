package uk.ac.ebi.uniprot.search.field;

/**
 * Created 13/05/19
 *
 * @author Edd
 */
public class SuggestField {
    public enum Search {
        content, id
    }

    public enum Stored {
        id, value, altValue
    }
}
