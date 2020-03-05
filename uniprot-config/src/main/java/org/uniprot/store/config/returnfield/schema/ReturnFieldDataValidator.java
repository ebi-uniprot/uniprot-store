package org.uniprot.store.config.returnfield.schema;

import org.uniprot.store.config.returnfield.model.ReturnField;
import org.uniprot.store.config.schema.AbstractFieldValidator;

import java.util.List;
import java.util.Set;

public class ReturnFieldDataValidator extends AbstractFieldValidator<ReturnField> {
    @Override
    public void validateContent(List<ReturnField> fieldItems) {
        Set<String> ids = extractIds(fieldItems);
        validateParentExists(fieldItems, ids);
        validateSeqNumbers(fieldItems);
        validateChildNumbers(fieldItems);
    }
}
