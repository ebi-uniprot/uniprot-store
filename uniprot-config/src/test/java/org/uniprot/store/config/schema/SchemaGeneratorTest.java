package org.uniprot.store.config.schema;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.uniprot.store.config.model.ConfigFieldItem;

public class SchemaGeneratorTest {
    @DisplayName("Test schema generation")
    @Test
    void testSchemaGeneration() {
        String schema = SchemaGenerator.generateSchema(ConfigFieldItem[].class);
        Assertions.assertNotNull(schema);
    }
}
