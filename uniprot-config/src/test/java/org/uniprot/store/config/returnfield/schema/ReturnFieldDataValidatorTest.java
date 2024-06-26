package org.uniprot.store.config.returnfield.schema;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.uniprot.store.config.returnfield.model.ReturnField;
import org.uniprot.store.config.returnfield.model.ReturnFieldItemType;
import org.uniprot.store.config.schema.SchemaValidationException;

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
                                List.of(invalidField, validField)));
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

    @Test
    void doNotHaveDuplicatedNamesReturnSuccess() {
        ReturnField validField1 = new ReturnField();
        validField1.setName("name1");
        validField1.setAliases(List.of("alias1"));

        ReturnField validField2 = new ReturnField();
        validField2.setName("name2");
        validField2.setAliases(List.of("alias2"));

        assertDoesNotThrow(
                () -> validator.mustNotHaveDuplicatedNames(List.of(validField1, validField2)));
    }

    @Test
    void duplicatedFieldNamesCausesException() {
        ReturnField returnField = new ReturnField();
        returnField.setId("id");
        returnField.setName("name1");

        SchemaValidationException error =
                assertThrows(
                        SchemaValidationException.class,
                        () ->
                                validator.mustNotHaveDuplicatedNames(
                                        List.of(returnField, returnField)));
        assertEquals("Must have unique name. Duplicated values are: name1", error.getMessage());
    }

    @Test
    void duplicatedAliasCausesException() {
        ReturnField returnField = new ReturnField();
        returnField.setId("id");
        returnField.setAliases(List.of("alias1"));

        SchemaValidationException error =
                assertThrows(
                        SchemaValidationException.class,
                        () ->
                                validator.mustNotHaveDuplicatedNames(
                                        List.of(returnField, returnField)));
        assertEquals("Must have unique alias. Duplicated values are: alias1", error.getMessage());
    }

    @Test
    void duplicatedAliasAndFieldsCausesException() {
        String duplicatedValue = "name";
        ReturnField returnFieldName = new ReturnField();
        returnFieldName.setId("id1");
        returnFieldName.setName(duplicatedValue);

        ReturnField returnFieldAlias = new ReturnField();
        returnFieldAlias.setId("id2");
        returnFieldAlias.setAliases(List.of(duplicatedValue));

        SchemaValidationException error =
                assertThrows(
                        SchemaValidationException.class,
                        () ->
                                validator.mustNotHaveDuplicatedNames(
                                        List.of(returnFieldName, returnFieldAlias)));
        assertEquals(
                "Must have unique alias and name. Duplicated values are: " + duplicatedValue,
                error.getMessage());
    }
}
