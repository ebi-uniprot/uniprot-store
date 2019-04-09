package uk.ac.ebi.uniprot.indexer.document.field;

/**
 *
 * @author lgonzales
 */
public interface SearchField {

    Float getBoostValue();

    boolean hasValidValue(String value);

    String getName();

}
