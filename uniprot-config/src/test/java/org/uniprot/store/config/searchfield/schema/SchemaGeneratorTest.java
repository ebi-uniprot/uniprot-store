package org.uniprot.store.config.searchfield.schema;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.uniprot.store.config.searchfield.model.FieldItem;

public class SchemaGeneratorTest {
    @DisplayName("Test schema generation")
    @Test
    void testSchemaGeneration() {
        String schema = SchemaGenerator.generateSchema(FieldItem[].class);
        Assertions.assertNotNull(schema);
    }
}
