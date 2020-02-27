package org.uniprot.store.config.searchfield.schema;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.uniprot.store.config.searchfield.model.SearchFieldItem;

public class SchemaGeneratorTest {
    @DisplayName("Test schema generation")
    @Test
    void testSchemaGeneration() {
        String schema = SchemaGenerator.generateSchema(SearchFieldItem[].class);
        Assertions.assertNotNull(schema);
    }
}
