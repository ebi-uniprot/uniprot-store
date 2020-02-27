package org.uniprot.store.config.searchfield.common;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import org.uniprot.store.config.searchfield.model.SearchFieldItem;

import com.fasterxml.jackson.databind.ObjectMapper;

@Slf4j
public class SearchFieldConfigLoader {

    public InputStream readConfig(String config) {
        InputStream inputStream =
                SearchFieldConfig.class.getClassLoader().getResourceAsStream(config);
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

    public List<SearchFieldItem> loadAndGetFieldItems(@NonNull String configFile) {
        ObjectMapper objectMapper = new ObjectMapper();
        List<SearchFieldItem> fieldItemList;
        try (InputStream inputStream = readConfig(configFile)) {
            if (inputStream == null) {
                throw new IllegalArgumentException("File '" + configFile + "' not found");
            }
            fieldItemList =
                    Arrays.asList(objectMapper.readValue(inputStream, SearchFieldItem[].class));
        } catch (IOException e) {
            log.error(e.getMessage());
            throw new IllegalArgumentException(
                    "File '"
                            + configFile
                            + "' could not be be converted into list of SearchFieldItem");
        }
        return fieldItemList;
    }

    public Map<String, SearchFieldItem> buildIdFieldItemMap(
            @NonNull List<SearchFieldItem> fieldItems) {
        return fieldItems.stream()
                .collect(Collectors.toMap(SearchFieldItem::getId, fieldItem -> fieldItem));
    }
}
