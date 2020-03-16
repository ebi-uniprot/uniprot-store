package org.uniprot.store.config.returnfield.schema;

import java.util.List;
import java.util.stream.Collectors;

import org.uniprot.store.config.returnfield.model.ReturnField;
import org.uniprot.store.config.returnfield.model.ReturnFieldItemType;
import org.uniprot.store.config.schema.FieldDataValidator;

/**
 * Created 16/03/20
 *
 * @author Edd
 */
public class ReturnFieldDataValidator extends FieldDataValidator<ReturnField> {
    @Override
    protected List<ReturnField> extractParentNodes(List<ReturnField> fieldItems) {
        return fieldItems.stream()
                .filter(
                        field ->
                                field.getItemType() != null
                                        && field.getItemType().equals(ReturnFieldItemType.GROUP))
                .collect(Collectors.toList());
    }
}
