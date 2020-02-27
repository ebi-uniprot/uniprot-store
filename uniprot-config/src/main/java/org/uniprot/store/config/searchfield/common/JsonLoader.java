package org.uniprot.store.config.searchfield.common;

import java.io.InputStream;
import java.util.List;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * This class is responsible for loading JSON objects and creating an instance of the corresponding
 * model.
 *
 * <p>Created 15/11/19
 *
 * @author Edd
 */
public class JsonLoader {
    private JsonLoader() {}

    public static <T> List<T> loadItems(String fileName, ObjectMapper mapper, JavaType type) {
        List<T> allItems;
        try (InputStream is = JsonLoader.class.getClassLoader().getResourceAsStream(fileName); ) {
            allItems = mapper.readValue(is, type);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return allItems;
    }
}
