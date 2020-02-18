//package org.uniprot.store.config.common;
//
//import java.io.*;
//import java.util.Arrays;
//import java.util.List;
//import java.util.Map;
//import java.util.function.Predicate;
//import java.util.stream.Collectors;
//
//import lombok.NonNull;
//import lombok.extern.slf4j.Slf4j;
//
//import org.uniprot.store.config.model.FieldItem;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import org.uniprot.store.config.schema.DataValidator;
//import org.uniprot.store.config.schema.SchemaValidator;
//
//@Slf4j
//public abstract class AbstractFieldConfiguration implements ConfigFieldService {
//    private String configSchema;
//    private String configFile;
//    private List<FieldItem> fieldItems;
//    private Map<String, FieldItem> idFieldItemMap;
//
//    protected AbstractFieldConfiguration(String configSchema, String configFile) {
//        this.configSchema = configSchema;
//        this.configFile = configFile;
//        SchemaValidator.validate(configSchema, configFile);
//        init();
//        DataValidator.validateContent(this.fieldItems, idFieldItemMap);
//    }
//
//    public List<FieldItem> getAllFieldItems() {
//        return this.fieldItems;
//    }
//
//    public FieldItem getFieldItemById(@NonNull String id) {
//        return this.idFieldItemMap.get(id);
//    }
//
//    private void init() {
//        this.fieldItems = loadAndGetFieldItems(this.configFile);
//        this.idFieldItemMap = buildIdFieldItemMap(this.fieldItems);
//    }
//
//    private List<FieldItem> loadAndGetFieldItems(@NonNull String config) {
//        ObjectMapper objectMapper = new ObjectMapper();
//        List<FieldItem> fieldItemList;
//        try (InputStream inputStream = readConfig(config)) {
//            if (inputStream == null) {
//                throw new IllegalArgumentException("File '" + config + "' not found");
//            }
//            fieldItemList = Arrays.asList(objectMapper.readValue(inputStream, FieldItem[].class));
//        } catch (IOException e) {
//            log.error(e.getMessage());
//            throw new IllegalArgumentException(
//                    "File '" + config + "' could not be be converted into list of FieldItem");
//        }
//        return fieldItemList;
//    }
//
//    private Map<String, FieldItem> buildIdFieldItemMap(@NonNull List<FieldItem> fieldItems) {
//        return fieldItems.stream()
//                .collect(Collectors.toMap(FieldItem::getId, fieldItem -> fieldItem));
//    }
//
//    private InputStream readConfig(String config) {
//        InputStream inputStream =
//                FieldConfiguration.class.getClassLoader().getResourceAsStream(config);
//        if (inputStream == null) {
//            File file = new File(config);
//            try {
//                inputStream = new FileInputStream(file);
//            } catch (FileNotFoundException e) {
//                // Do nothing. The caller is throwing IAE
//            }
//        }
//        return inputStream;
//    }
//}
