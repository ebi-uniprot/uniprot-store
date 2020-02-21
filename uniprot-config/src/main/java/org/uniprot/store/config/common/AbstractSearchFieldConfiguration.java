package org.uniprot.store.config.common;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import org.uniprot.store.config.model.FieldItem;
import org.uniprot.store.config.schema.DataValidator;
import org.uniprot.store.config.schema.SchemaValidator;

import com.fasterxml.jackson.databind.ObjectMapper;

@Slf4j
public abstract class AbstractSearchFieldConfiguration implements SearchFieldConfiguration {
    private List<FieldItem> fieldItems;
    private Map<String, FieldItem> idFieldItemMap;
    private String schemaFile;
    private String configFile;

    protected AbstractSearchFieldConfiguration(String schemaFile, String configFile) {
        SchemaValidator.validate(schemaFile, configFile);
        init(schemaFile, configFile);
        DataValidator.validateContent(this.fieldItems, idFieldItemMap);
    }

    private void init(String schemaFile, String configFile) {
        this.schemaFile = schemaFile;
        this.configFile = configFile;
        this.fieldItems = loadAndGetFieldItems(this.configFile);
        this.idFieldItemMap = buildIdFieldItemMap(this.fieldItems);
    }

    public List<FieldItem> getAllFieldItems() {
        return this.fieldItems;
    }

    public FieldItem getFieldItemById(@NonNull String id) {
        return this.idFieldItemMap.get(id);
    }

    public List<FieldItem> loadAndGetFieldItems(@NonNull String configFile) {
        ObjectMapper objectMapper = new ObjectMapper();
        List<FieldItem> fieldItemList;
        try (InputStream inputStream = readConfig(configFile)) {
            if (inputStream == null) {
                throw new IllegalArgumentException("File '" + configFile + "' not found");
            }
            fieldItemList = Arrays.asList(objectMapper.readValue(inputStream, FieldItem[].class));
        } catch (IOException e) {
            log.error(e.getMessage());
            throw new IllegalArgumentException(
                    "File '" + configFile + "' could not be be converted into list of FieldItem");
        }
        return fieldItemList;
    }

    public Map<String, FieldItem> buildIdFieldItemMap(@NonNull List<FieldItem> fieldItems) {
        return fieldItems.stream()
                .collect(Collectors.toMap(FieldItem::getId, fieldItem -> fieldItem));
    }

    public InputStream readConfig(String config) {
        InputStream inputStream =
                SearchFieldConfiguration.class.getClassLoader().getResourceAsStream(config);
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
