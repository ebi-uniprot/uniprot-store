package org.uniprot.store.config.schema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kjetland.jackson.jsonSchema.JsonSchemaConfig;
import com.kjetland.jackson.jsonSchema.JsonSchemaGenerator;

public class SchemaGenerator {

    public static String generateSchema(Class clazz) {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonSchemaConfig config =
                JsonSchemaConfig.vanillaJsonSchemaDraft4().withFailOnUnknownProperties(true);
        JsonSchemaGenerator jsonSchemaGenerator = new JsonSchemaGenerator(objectMapper, config);
        JsonNode jsonSchema = jsonSchemaGenerator.generateJsonSchema(clazz);

        String schema;
        try {
            schema = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonSchema);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Unable to generate schema");
        }
        return schema;
    }
}
