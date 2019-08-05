package org.uniprot.store.search.field;

/**
 * @author lgonzales
 */
public interface ReturnField {

    boolean hasReturnField(String fieldName);
    default String getJavaFieldName(){
        return "";
    }

    // will the field to be returned in Json response in all cases?
    default boolean isMandatoryJsonField(){
        return false;
    }
}
