package org.uniprot.store.config.schema;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

@Slf4j
public class SchemaValidator {
    public static void validate(@NonNull String schemaFile, @NonNull String jsonInputFile) {
        InputStream schemaStream = readFile(schemaFile);
        InputStream jsonStream = readFile(jsonInputFile);

        validateInputAgainstSchema(schemaStream, jsonStream);
    }

    private static void validateInputAgainstSchema(
            InputStream schemaStream, InputStream configStream) {
        JSONObject jsonSchema = new JSONObject(new JSONTokener(schemaStream));

        JSONArray jsonInput = new JSONArray(new JSONTokener(configStream));

        SchemaLoader loader =
                SchemaLoader.builder().schemaJson(jsonSchema).draftV7Support().build();
        Schema schema = loader.load().build();
        try {
            schema.validate(jsonInput);
        } catch (ValidationException ve) {
            log.error(ve.getAllMessages().toString());
            throw new SchemaValidationException("Schema validation failed", ve);
        }
    }

    private static InputStream readFile(String filePath) {
        InputStream inputStream =
                SchemaValidator.class.getClassLoader().getResourceAsStream(filePath);
        if (inputStream == null) {
            File file = new File(filePath);
            try {
                inputStream = new FileInputStream(file);
            } catch (FileNotFoundException e) {
                log.error(e.getMessage());
                throw new IllegalArgumentException("Unable to read the file " + filePath);
            }
        }
        return inputStream;
    }

    private SchemaValidator() {}
}
