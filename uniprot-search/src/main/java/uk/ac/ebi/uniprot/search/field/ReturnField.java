package uk.ac.ebi.uniprot.search.field;

/**
 * @author lgonzales
 */
public interface ReturnField {

    boolean hasReturnField(String fieldName);
    default String getJavaFieldName(){
        return null;
    }
}
