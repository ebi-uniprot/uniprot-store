package org.uniprot.store.config.returnfield.config;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import org.uniprot.core.util.Utils;
import org.uniprot.store.config.common.JsonLoader;
import org.uniprot.store.config.returnfield.model.ReturnField;
import org.uniprot.store.config.returnfield.schema.ReturnFieldDataValidator;
import org.uniprot.store.config.schema.SchemaValidator;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

@Slf4j
public abstract class AbstractReturnFieldConfig implements ReturnFieldConfig {
    public static final String SCHEMA_FILE = "schema/return-fields-schema.json";

    protected List<ReturnField> allFields;
    private List<ReturnField> returnFields;

    public AbstractReturnFieldConfig(String configFile) {
        SchemaValidator.validate(SCHEMA_FILE, configFile);
        init(configFile);
        new ReturnFieldDataValidator().validateContent(this.allFields);
    }

    @Override
    public List<ReturnField> getAllFields() {
        return this.allFields;
    }

    @Override
    public List<ReturnField> getReturnFields() {
        if (this.returnFields == null) {
            this.returnFields =
                    getAllFields().stream()
                            .filter(this::isReturnField)
                            .collect(Collectors.toList());
        }
        return this.returnFields;
    }

    @Override
    public List<ReturnField> getDefaultReturnFields() {
        return this.getReturnFields().stream()
                .filter(ReturnField::getIsDefaultForTsv)
                .sorted(Comparator.comparing(ReturnField::getDefaultForTsvOrder))
                .collect(Collectors.toList());
    }

    @Override
    public List<ReturnField> getRequiredReturnFields() {
        return this.getReturnFields().stream()
                .filter(ReturnField::getIsRequiredForJson)
                .collect(Collectors.toList());
    }

    @Override
    public ReturnField getReturnFieldByName(String fieldName) {
        return this.getReturnFields().stream()
                .filter(field -> fieldName.equals(field.getName()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown field: " + fieldName));
    }

    @Override
    public boolean returnFieldExists(String fieldName) {
        boolean searchFieldExist = false;
        try {
            searchFieldExist = Objects.nonNull(this.getReturnFieldByName(fieldName));
        } catch (IllegalArgumentException ile) {
            // it means that the field doesn't exist
        }
        return searchFieldExist;
    }

    protected abstract Collection<ReturnField> dynamicallyLoadFields();

    private void init(String configFile) {
        ObjectMapper mapper = new ObjectMapper();
        JavaType type =
                mapper.getTypeFactory().constructCollectionType(List.class, ReturnField.class);

        this.allFields = JsonLoader.loadItems(configFile, mapper, type);
        this.allFields.addAll(dynamicallyLoadFields());
    }

    private boolean isReturnField(ReturnField returnField) {
        return Utils.nullOrEmpty(returnField.getGroupName());
    }
}
