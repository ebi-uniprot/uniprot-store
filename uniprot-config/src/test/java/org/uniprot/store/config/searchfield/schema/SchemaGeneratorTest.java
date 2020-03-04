package org.uniprot.store.config.schema;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.uniprot.store.config.uniprotkb.CreateJsonForResultFields;

import java.io.FileWriter;
import java.io.IOException;

public class SchemaGeneratorTest {
    @DisplayName("Test schema generation")
    @Test
    void testSchemaGeneration() {
//        String schema = SchemaGenerator.generateSchema(FieldItem[].class);
        String schema = SchemaGenerator.generateSchema(CreateJsonForResultFields.InternalResultField[].class);
        try {
            FileWriter myWriter = new FileWriter("filename.txt");
            myWriter.write(schema);
            myWriter.close();
            System.out.println("Successfully wrote to the file.");
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
        Assertions.assertNotNull(schema);
    }
}
