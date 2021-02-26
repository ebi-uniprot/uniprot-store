package org.uniprot.store.config.idmapping;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.xdb.UniProtDatabaseDetail;

/**
 * @author sahmad
 * @created 26/02/2021
 */
class IdMappingFieldConfigTest {
    @Test
    void testGetAllIdMappingFields() {
        List<UniProtDatabaseDetail> idMappingFields = IdMappingFieldConfig.getAllIdMappingTypes();
        Assertions.assertNotNull(idMappingFields);
        Assertions.assertEquals(98, idMappingFields.size());
        idMappingFields.stream()
                .forEach(field -> Assertions.assertNotNull(field.getIdMappingName()));
        // verify few mapped fields
        Set<String> names =
                idMappingFields.stream()
                        .map(UniProtDatabaseDetail::getIdMappingName)
                        .collect(Collectors.toSet());
        Assertions.assertTrue(names.contains("ACC"));
        Assertions.assertTrue(names.contains("NF100"));
        Assertions.assertTrue(names.contains("EMBL"));
    }
}
