package org.uniprot.store.config.returnfield.common;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.uniprot.core.util.Utils;
import org.uniprot.store.config.returnfield.model.ReturnField;
import org.uniprot.store.config.returnfield.schema.DataValidator;
import org.uniprot.store.config.schema.SchemaValidator;
import org.uniprot.store.config.searchfield.common.JsonLoader;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public abstract class AbstractReturnFieldConfig implements ReturnFieldConfig {
    public static final String SCHEMA_FILE = "schema/result-fields-schema.json";

    private List<ReturnField> allFields;
    private List<ReturnField> returnFields;
    private Set<String> ids;

    protected AbstractReturnFieldConfig(String schemaFile, String configFile) {
        SchemaValidator.validate(schemaFile, configFile);
        init();
        DataValidator.validateContent(this.allFields, ids);
    }

    private void init() {
        ObjectMapper mapper = new ObjectMapper();
        JavaType type =
                mapper.getTypeFactory().constructCollectionType(List.class, ReturnField.class);

        this.allFields = JsonLoader.loadItems(SCHEMA_FILE, mapper, type);
        ids = this.allFields.stream().map(ReturnField::getId).collect(Collectors.toSet());
    }

    public List<ReturnField> getAllFieldItems() {
        return this.allFields;
    }

    @Override
    public List<ReturnField> getReturnFields() {
        if (this.returnFields == null) {
            this.returnFields =
                    getAllFieldItems().stream()
                            .filter(this::isReturnField)
                            .collect(Collectors.toList());
        }
        return this.returnFields;
    }

    private boolean isReturnField(ReturnField returnField) {
        return Utils.notNullNotEmpty(returnField.getGroupName());
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
            // it means, search field doesn't exist
        }
        return searchFieldExist;
    }
}
