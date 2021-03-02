package org.uniprot.store.config.idmapping;

import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.xdb.UniProtDatabaseDetail;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.uniprot.store.config.idmapping.IdMappingFieldConfig.*;

/**
 * Created 26/02/2021
 *
 * @author sahmad
 */
class IdMappingFieldConfigTest {
    @Test
    void testGetAllIdMappingFields() {
        List<UniProtDatabaseDetail> idMappingFields = IdMappingFieldConfig.getAllIdMappingTypes();
        Assertions.assertNotNull(idMappingFields);
        // TODO: 02/03/2021 i'm not sure if we should test exact size... because
        // drlineconfiguration.json is subject to change
        Assertions.assertEquals(98, idMappingFields.size());
        idMappingFields.forEach(field -> Assertions.assertNotNull(field.getIdMappingName()));
        // verify few mapped fields
        Set<String> names =
                idMappingFields.stream()
                        .map(UniProtDatabaseDetail::getIdMappingName)
                        .collect(Collectors.toSet());
        Assertions.assertTrue(names.contains("ACC"));
        Assertions.assertTrue(names.contains("NF100"));
        Assertions.assertTrue(names.contains("EMBL"));
    }

    @Test
    void canMapDbNameToPIRDbName() {
        MatcherAssert.assertThat(
                IdMappingFieldConfig.convertDbNameToPIRDbName("UniProtKB Accession"),
                CoreMatchers.is(ACC_STR));

        MatcherAssert.assertThat(
                IdMappingFieldConfig.convertDbNameToPIRDbName("UniProtKB AC/ID"),
                CoreMatchers.is(ACC_ID_STR));

        MatcherAssert.assertThat(
                IdMappingFieldConfig.convertDbNameToPIRDbName("Gene Name"),
                CoreMatchers.is(GENENAME_STR));

        MatcherAssert.assertThat(
                IdMappingFieldConfig.convertDbNameToPIRDbName("GenBank"),
                CoreMatchers.is("EMBL_ID"));
    }
}
