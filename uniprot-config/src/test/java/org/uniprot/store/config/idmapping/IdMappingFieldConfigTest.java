package org.uniprot.store.config.idmapping;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.xdb.UniProtDatabaseDetail;

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
                IdMappingFieldConfig.convertDbNameToPIRDbName("UniProtKB"), CoreMatchers.is("ACC"));

        MatcherAssert.assertThat(
                IdMappingFieldConfig.convertDbNameToPIRDbName("UniProtKB_AC-ID"),
                CoreMatchers.is("ACC,ID"));

        MatcherAssert.assertThat(
                IdMappingFieldConfig.convertDbNameToPIRDbName("Gene_Name"),
                CoreMatchers.is("GENENAME"));

        MatcherAssert.assertThat(
                IdMappingFieldConfig.convertDbNameToPIRDbName("EMBL-GenBank-DDBJ"),
                CoreMatchers.is("EMBL_ID"));

        MatcherAssert.assertThat(
                IdMappingFieldConfig.convertDbNameToPIRDbName("RefSeq_Protein"),
                CoreMatchers.is("P_REFSEQ_AC"));

        MatcherAssert.assertThat(
                IdMappingFieldConfig.convertDbNameToPIRDbName("RefSeq_Nucleotide"),
                CoreMatchers.is("REFSEQ_NT_ID"));
    }

    @Test
    void convertsDisplayNameCorrectly() {
        MatcherAssert.assertThat(
                IdMappingFieldConfig.convertDisplayNameToName("More complex (db1/db2)"),
                CoreMatchers.is("More_complex_-db1-db2-"));
    }
}
