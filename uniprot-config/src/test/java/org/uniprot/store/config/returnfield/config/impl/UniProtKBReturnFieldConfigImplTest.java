package org.uniprot.store.config.returnfield.config.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;
import org.uniprot.core.cv.xdb.UniProtDatabaseCategory;
import org.uniprot.core.cv.xdb.UniProtDatabaseDetail;
import org.uniprot.cv.xdb.UniProtDatabaseTypes;
import org.uniprot.store.config.returnfield.model.ReturnField;
import org.uniprot.store.config.returnfield.model.ReturnFieldItemType;

/**
 * Created 13/03/20
 *
 * @author Edd
 */
class UniProtKBReturnFieldConfigImplTest {

    private static UniProtKBReturnFieldConfigImpl config;

    @BeforeAll
    static void setUp() {
        config = new UniProtKBReturnFieldConfigImpl("test-return-fields.json");
    }

    @Test
    void dynamicallyLoadedFields() {
        assertThat(
                config.getReturnFields().stream()
                        .filter(field -> field.getParentId().equals("genome_annotation/group"))
                        .collect(Collectors.toList()),
                hasSize(greaterThan(0)));
    }

    @Test
    void loadMultiValueCrossReferenceWithMoreThanOnePropertyReturnTrue() {
        ReturnField returnField =
                config.getReturnFields().stream()
                        .filter(field -> field.getName().equals("xref_ensembl"))
                        .findFirst()
                        .orElseThrow(AssertionFailedError::new);
        assertThat(returnField, notNullValue());
        assertThat(returnField.getIsMultiValueCrossReference(), is(true));
    }

    @Test
    void loadMultiValueCrossReferenceWithOnePropertyReturnTrue() {
        ReturnField returnField =
                config.getReturnFields().stream()
                        .filter(field -> field.getName().equals("xref_patric"))
                        .findFirst()
                        .orElseThrow(AssertionFailedError::new);
        assertThat(returnField, notNullValue());
        assertThat(returnField.getName(), is("xref_patric"));
        assertThat(returnField.getIsMultiValueCrossReference(), is(true));
    }

    @Test
    void loadMultiValueCrossReferenceWithOneDefaultPropertyReturnFalse() {
        ReturnField returnField =
                config.getReturnFields().stream()
                        .filter(field -> field.getName().equals("xref_kegg"))
                        .findFirst()
                        .orElseThrow(AssertionFailedError::new);
        assertThat(returnField, notNullValue());
        assertThat(returnField.getName(), is("xref_kegg"));
        assertThat(returnField.getIsMultiValueCrossReference(), is(false));
    }

    @Test
    void getReturnFieldByNameForValidFullMultiValueReturnFieldWithCorrectFullName() {
        ReturnField fullField = config.getReturnFieldByName("xref_ensembl_full");
        assertThat(fullField, notNullValue());
        assertThat(fullField.getName(), is("xref_ensembl_full"));
        assertThat(fullField.getIsMultiValueCrossReference(), is(true));
    }

    @Test
    void getReturnFieldByNameForValidFullMultiValueReturnFieldWithCorrectName() {
        ReturnField field = config.getReturnFieldByName("xref_ensembl");
        assertThat(field, notNullValue());
        assertThat(field.getName(), is("xref_ensembl"));
        assertThat(field.getIsMultiValueCrossReference(), is(true));
    }

    @Test
    void getReturnFieldByNameForInvalidFullMultiValueThrowsIllegalArgumentException() {
        IllegalArgumentException error =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> config.getReturnFieldByName("xref_kegg_full"));
        assertThat(error, notNullValue());
        assertThat(error.getMessage(), is("Unknown field: xref_kegg_full"));
    }

    @Test
    void getReturnFieldByNameForInvalidFieldNameThrowsIllegalArgumentException() {
        IllegalArgumentException error =
                assertThrows(
                        IllegalArgumentException.class, () -> config.getReturnFieldByName("XXXX"));
        assertThat(error, notNullValue());
        assertThat(error.getMessage(), is("Unknown field: XXXX"));
    }

    @Test
    void unknownDatabaseCategoryCausesException() {
        assertThrows(IllegalArgumentException.class, () -> config.getDatabaseCategory("XXXX"));
    }

    @Test
    void checkReturnFieldContainsCorrectAttributes() {

        UniProtDatabaseDetail detail =
                UniProtDatabaseTypes.INSTANCE
                        .getDBTypesByCategory(UniProtDatabaseCategory.FAMILY_AND_DOMAIN_DATABASES)
                        .stream()
                        .findFirst()
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Please update test with valid UniProtDatabaseCategory"));

        ReturnField parent = new ReturnField();
        parent.setId("parentId");
        AtomicInteger counter = new AtomicInteger();
        int childNumber = counter.get();
        ReturnField returnField = config.databaseToReturnField(detail, parent, counter);

        String dbName = detail.getName();
        String dbNameLowercase = dbName.toLowerCase();

        assertThat(returnField.getName(), is("xref_" + dbNameLowercase));
        assertThat(returnField.getSeqNumber(), is(nullValue()));
        assertThat(returnField.getParentId(), is(parent.getId()));
        assertThat(returnField.getChildNumber(), is(childNumber));
        assertThat(returnField.getItemType(), is(ReturnFieldItemType.SINGLE));
        assertThat(returnField.getLabel(), is(dbName));
        assertThat(
                returnField.getPaths(),
                hasItems("uniProtKBCrossReferences[?(@.database=='" + dbName + "')]"));
        assertThat(returnField.getGroupName(), is(nullValue()));
        assertThat(returnField.getIsDatabaseGroup(), is(false));
        assertThat(returnField.getIsMultiValueCrossReference(), is(true));
        assertThat(returnField.getId(), is(parent.getId() + "/" + dbNameLowercase));
    }
}
