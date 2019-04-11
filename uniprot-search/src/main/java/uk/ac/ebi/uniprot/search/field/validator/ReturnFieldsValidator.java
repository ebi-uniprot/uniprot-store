package uk.ac.ebi.uniprot.search.field.validator;

/**
 * This interface is responsible to define methods that are used to validate returned fields in the request
 * This is used in conjunction with @ValidReturnFields request validator
 *
 * @author lgonzales
 */
public interface ReturnFieldsValidator {

    boolean hasValidReturnField(String fieldName);

}
