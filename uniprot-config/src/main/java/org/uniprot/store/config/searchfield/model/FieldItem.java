package org.uniprot.store.config.searchfield.model;

import java.io.Serializable;
import java.util.List;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.PositiveOrZero;

import lombok.Data;

/** @author sahmad */
@Data
public class FieldItem implements Serializable {
    private static final long serialVersionUID = -1835246966684124878L;
    @NotNull private String id;
    private String parentId;
    @PositiveOrZero private Integer childNumber;
    @NotNull @PositiveOrZero private Integer seqNumber;
    @NotNull private ItemType itemType;
    private FieldType fieldType;
    private String label;
    private String fieldName;
    private DataType dataType;
    private String description;
    private String example;
    private String validRegex;
    private List<Value> values;
    private String autoComplete;
    private String autoCompleteQueryField;
    private String autoCompleteQueryFieldValidRegex;
    private String sortFieldId;

    @Data
    public static class Value {
        private String name;
        private String value;
    }
}
