package org.uniprot.store.config.common;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import org.uniprot.store.config.model.FieldItem;

import com.fasterxml.jackson.databind.ObjectMapper;

@Slf4j
public abstract class AbstractFieldConfiguration implements FieldConfiguration {

    public List<FieldItem> loadAndGetFieldItems(@NonNull String config) {
        ObjectMapper objectMapper = new ObjectMapper();
        List<FieldItem> fieldItemList;
        try (InputStream inputStream = readConfig(config)) {
            if (inputStream == null) {
                throw new IllegalArgumentException("File '" + config + "' not found");
            }
            fieldItemList = Arrays.asList(objectMapper.readValue(inputStream, FieldItem[].class));
        } catch (IOException e) {
            log.error(e.getMessage());
            throw new IllegalArgumentException(
                    "File '" + config + "' could not be be converted into list of FieldItem");
        }
        return fieldItemList;
    }

    public Map<String, FieldItem> buildIdFieldItemMap(@NonNull List<FieldItem> fieldItems) {
        return fieldItems.stream()
                .collect(Collectors.toMap(FieldItem::getId, fieldItem -> fieldItem));
    }

    public InputStream readConfig(String config) {
        InputStream inputStream =
                FieldConfiguration.class.getClassLoader().getResourceAsStream(config);
        if (inputStream == null) {
            File file = new File(config);
            try {
                inputStream = new FileInputStream(file);
            } catch (FileNotFoundException e) {
                // Do nothing. The caller is throwing IAE
            }
        }
        return inputStream;
    }

    @Override
    public List<FieldItem> getTopLevelFieldItems() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<FieldItem> getChildFieldItems(String parentId) {
        throw new UnsupportedOperationException();
    }
}
