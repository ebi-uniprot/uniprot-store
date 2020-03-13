package org.uniprot.store.config.returnfield.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import org.uniprot.store.config.model.Field;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.PositiveOrZero;
import java.io.Serializable;
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
    @NotNull private ResultFieldItemType itemType;
    private String name;
    private String label;
    private String path;
    private String filter;
    private String groupName;
    private Boolean isDatabaseGroup = false;
    @NotNull private String id;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReturnField that = (ReturnField) o;
//        if (((ReturnField) this).getLabel() != null && ((ReturnField) this).getLabel().equals("CDD")) {
//            System.out.println("hello");
//        }
        return Objects.equals(seqNumber, that.seqNumber) &&
                Objects.equals(parentId, that.parentId) &&
//                Objects.equals(childNumber, that.childNumber) &&
                itemType == that.itemType &&
                Objects.equals(name, that.name) &&
                Objects.equals(label, that.label) &&
                Objects.equals(path, that.path) &&
                Objects.equals(filter, that.filter) &&
                Objects.equals(groupName, that.groupName) &&
                Objects.equals(isDatabaseGroup, that.isDatabaseGroup) &&
                Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects
                .hash(seqNumber, parentId, childNumber, itemType, name, label, path, filter, groupName, isDatabaseGroup, id);
    }
}
