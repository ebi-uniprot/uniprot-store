package org.uniprot.store.config.returnfield.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import org.uniprot.core.util.Utils;
import org.uniprot.store.config.model.Field;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.PositiveOrZero;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

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
    private List<String> paths;
    private String groupName;
    private Boolean isDatabaseGroup = false;
    @NotNull private String id;
    private String sortField;

    public void addPath(String path) {
        if (Utils.notNullNotEmpty(path)) {
            if (Objects.isNull(this.paths)) {
                this.paths = new ArrayList<>();
            }
            this.paths.add(path);
        }
    }
}
