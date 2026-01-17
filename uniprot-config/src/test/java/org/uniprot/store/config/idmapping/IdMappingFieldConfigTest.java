package org.uniprot.store.config.idmapping;

import static org.uniprot.store.config.idmapping.IdMappingFieldConfig.convertDisplayNameToName;

import java.util.*;
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
        Assertions.assertEquals(99, idMappingFields.size());
        idMappingFields.forEach(field -> Assertions.assertNotNull(field.getIdMappingName()));
        // verify few mapped fields
        Set<String> names =
                idMappingFields.stream()
                        .map(UniProtDatabaseDetail::getIdMappingName)
                        .collect(Collectors.toSet());
        Assertions.assertTrue(names.contains("ACC"));
        Assertions.assertTrue(names.contains("NF100"));
        Assertions.assertTrue(names.contains("EMBL"));
        Assertions.assertTrue(names.contains("OPENTARGETS_ID"));
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

        MatcherAssert.assertThat(
                IdMappingFieldConfig.convertDbNameToPIRDbName("OpenTargets"),
                CoreMatchers.is("OPENTARGETS_ID"));
    }

    @Test
    void convertsDisplayNameCorrectly() {
        MatcherAssert.assertThat(
                convertDisplayNameToName("More complex (db1/db2)"),
                CoreMatchers.is("More_complex_-db1-db2-"));
    }

    @Test
    void testNoDuplicateUniProtDatabaseDetail() {
        List<UniProtDatabaseDetail> idMappingFields = IdMappingFieldConfig.getAllIdMappingTypes();
        Assertions.assertEquals(99, idMappingFields.size());
        // add details again to have duplicate
        idMappingFields.addAll(IdMappingFieldConfig.createMissingIdMappingTypes());
        idMappingFields =
                idMappingFields.stream()
                        .map(
                                detail ->
                                        new UniProtDatabaseDetail(
                                                convertDisplayNameToName(detail.getDisplayName()),
                                                detail.getDisplayName(),
                                                detail.getCategory(),
                                                detail.getUriLink(),
                                                detail.getAttributes(),
                                                detail.isImplicit(),
                                                detail.getLinkedReason(),
                                                detail.getIdMappingName()))
                        .collect(Collectors.toList());
        Assertions.assertEquals(112, idMappingFields.size());
        // remove duplicate
        ArrayList<UniProtDatabaseDetail> uniqueUniProtDatabaseDetails =
                new ArrayList<>(new LinkedHashSet<>(idMappingFields));
        Assertions.assertEquals(99, uniqueUniProtDatabaseDetails.size());
    }
}
