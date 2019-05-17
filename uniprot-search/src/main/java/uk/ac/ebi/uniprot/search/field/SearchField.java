package uk.ac.ebi.uniprot.search.field;

/**
 *
 * @author lgonzales
 */
public interface SearchField {

    Float getBoostValue();

    boolean hasBoostValue();

    boolean hasValidValue(String value);

    String getName();

}
