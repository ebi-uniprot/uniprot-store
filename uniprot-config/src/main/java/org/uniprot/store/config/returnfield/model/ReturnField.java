package org.uniprot.store.config.returnfield.model;

import java.io.Serializable;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.PositiveOrZero;

import lombok.Data;

import org.uniprot.store.config.model.Field;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * @author lgonzales
 * @since 2020-02-25
 */
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ReturnField implements Field, Serializable {
    private static final long serialVersionUID = 606080616718758299L;

    @NotNull @PositiveOrZero private Integer seqNumber;
    private String parentId;
    @PositiveOrZero private Integer childNumber;
    @NotNull private ReturnFieldItemType itemType;
    private String name;
    private String label;
    private String path;
    private String filter;
    private String groupName;
    private Boolean isDatabaseGroup = false;
    @NotNull private String id;
}
