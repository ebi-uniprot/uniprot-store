package uk.ac.ebi.uniprot.search.field;

/**
 *
 * @author lgonzales
 */
public interface SearchField {

    Float getBoostValue();

    boolean hasValidValue(String value);

    String getName();

}
