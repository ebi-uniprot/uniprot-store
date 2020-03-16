package org.uniprot.store.config.returnfield.schema;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.uniprot.store.config.returnfield.model.ReturnField;
import org.uniprot.store.config.returnfield.model.ReturnFieldItemType;
import org.uniprot.store.config.schema.SchemaValidationException;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Created 16/03/20
 *
 * @author Edd
 */
class ReturnFieldDataValidatorTest {

    private static ReturnFieldDataValidator validator;

    @BeforeAll
    static void setUp() {
        validator = new ReturnFieldDataValidator();
    }

    @Test
    void nonSingleFieldWithSortFieldCausesException() {
        ReturnField invalidField = new ReturnField();
        invalidField.setItemType(ReturnFieldItemType.GROUP);
        invalidField.setId("invalid field's ID");
        invalidField.setSortField("sort field 1");

        ReturnField validField = new ReturnField();
        validField.setItemType(ReturnFieldItemType.SINGLE);
        validField.setId("valid field's ID");
        validField.setSortField("sort field 2");

        assertThrows(
                SchemaValidationException.class,
                () ->
                        validator.onlySingleFieldsCanHaveSortFields(
                                asList(invalidField, validField)));
    }

    @Test
    void singleFieldWithSortFieldCausesNoException() {
        ReturnField validField = new ReturnField();
        validField.setItemType(ReturnFieldItemType.SINGLE);
        validField.setId("valid field's ID");
        validField.setSortField("sort field 2");

        assertDoesNotThrow(
                () -> validator.onlySingleFieldsCanHaveSortFields(singletonList(validField)));
    }
}
