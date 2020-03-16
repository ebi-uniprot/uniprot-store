package org.uniprot.store.config.returnfield.config.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
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
                        .filter(field -> field.getParentId().equals("protein_family/group"))
                        .collect(Collectors.toList()),
                hasSize(greaterThan(0)));
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

        assertThat(returnField.getName(), is("dr_" + dbNameLowercase));
        assertThat(returnField.getSeqNumber(), is(nullValue()));
        assertThat(returnField.getParentId(), is(parent.getId()));
        assertThat(returnField.getChildNumber(), is(childNumber));
        assertThat(returnField.getItemType(), is(ReturnFieldItemType.SINGLE));
        assertThat(returnField.getLabel(), is(dbName));
        assertThat(returnField.getPath(), is("uniProtCrossReferences"));
        assertThat(returnField.getFilter(), is("[?(@.database=='" + dbName + "')]"));
        assertThat(returnField.getGroupName(), is(nullValue()));
        assertThat(returnField.getIsDatabaseGroup(), is(false));
        assertThat(returnField.getId(), is(parent.getId() + "/" + dbNameLowercase));
    }
}
