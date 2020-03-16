package org.uniprot.store.config.returnfield.schema;

import org.uniprot.core.util.Utils;
import org.uniprot.store.config.returnfield.model.ReturnField;
import org.uniprot.store.config.returnfield.model.ReturnFieldItemType;
import org.uniprot.store.config.schema.FieldDataValidator;
import org.uniprot.store.config.schema.SchemaValidationException;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Created 16/03/20
 *
 * @author Edd
 */
public class ReturnFieldDataValidator extends FieldDataValidator<ReturnField> {
    @Override
    public void validateContent(List<ReturnField> fieldItems) {
        super.validateContent(fieldItems);
        onlySingleFieldsCanHaveSortFields(fieldItems);
    }

    @Override
    protected List<ReturnField> extractParentNodes(List<ReturnField> fieldItems) {
        return fieldItems.stream()
                .filter(
                        field ->
                                field.getItemType() != null
                                        && field.getItemType().equals(ReturnFieldItemType.GROUP))
                .collect(Collectors.toList());
    }

    void onlySingleFieldsCanHaveSortFields(List<ReturnField> fieldItems) {
        List<String> invalidFields =
                fieldItems.stream()
                        .filter(field -> !field.getItemType().equals(ReturnFieldItemType.SINGLE))
                        .filter(field -> Utils.notNullNotEmpty(field.getSortField()))
                        .map(ReturnField::getId)
                        .collect(Collectors.toList());

        if (!invalidFields.isEmpty()) {
            throw new SchemaValidationException(
                    "Only SINGLE items can have a sort field. "
                            + "The following are non-SINGLE fields, with a sort field: "
                            + String.join(",", invalidFields));
        }
    }
}
