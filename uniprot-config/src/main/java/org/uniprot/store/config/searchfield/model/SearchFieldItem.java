package org.uniprot.store.config.searchfield.model;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.PositiveOrZero;

import lombok.Data;

import org.uniprot.store.config.model.Field;

/** @author sahmad */
@Data
public class SearchFieldItem implements Field, Serializable {
    private static final long serialVersionUID = -1835246966684124878L;
    private static final String CONTEXT_PATH_TOKEN = "{CONTEXT_PATH}";
    @NotNull private String id;
    private String parentId;
    @PositiveOrZero private Integer childNumber;
    @NotNull @PositiveOrZero private Integer seqNumber;
    @NotNull private SearchFieldItemType itemType;
    private SearchFieldType fieldType;
    private String label;
    private String fieldName;
    private SearchFieldDataType dataType;
    private String description;
    private String example;
    private String validRegex;
    private List<Value> values;
    private String autoComplete;
    private String autoCompleteQueryField;
    private String autoCompleteQueryFieldValidRegex;
    private String sortFieldId;
    private boolean includeInSwagger;

    @Data
    public static class Value implements Serializable {
        private static final long serialVersionUID = -9202109334799936104L;
        private String name;
        private String value;
    }

    public String getAutoComplete(String contextPath) {
        return Objects.nonNull(this.autoComplete)
                ? this.autoComplete.replace(CONTEXT_PATH_TOKEN, contextPath)
                : null;
    }

    public String getAutoComplete() {
        return getAutoComplete("");
    }
}
