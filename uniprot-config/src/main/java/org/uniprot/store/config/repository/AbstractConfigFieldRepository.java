package org.uniprot.store.config.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.uniprot.store.config.model.FieldItem;
import org.uniprot.store.config.schema.SchemaValidator;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public  abstract class AbstractConfigFieldRepository implements ConfigFieldRepository {

    private String configSchema;
    private String configFile;
    private List<FieldItem> fieldItems;
    private Map<String, FieldItem> idFieldItemMap;

    protected AbstractConfigFieldRepository(String configSchema, String configFile) {
        this.configSchema = configSchema;
        this.configFile = configFile;
        SchemaValidator.validate(configSchema, configFile);
        init();
        DataValidator.validateContent(this.fieldItems, idFieldItemMap);
    }

    public void init() {
        this.fieldItems = loadAndGetFieldItems(this.configFile);
        this.idFieldItemMap = buildIdFieldItemMap(this.fieldItems);
    }

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
                AbstractConfigFieldRepository.class.getClassLoader().getResourceAsStream(config);
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
    public List<FieldItem> getFieldItems() {
        return this.fieldItems;
    }

    @Override
    public Map<String, FieldItem> getIdFieldItemMap() {
        return this.idFieldItemMap;
    }
}

