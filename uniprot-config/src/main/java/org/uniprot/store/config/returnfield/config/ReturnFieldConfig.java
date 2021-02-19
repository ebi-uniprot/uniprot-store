package org.uniprot.store.config.returnfield.config;

import java.io.Serializable;
import java.util.List;

import org.uniprot.store.config.returnfield.model.ReturnField;

public interface ReturnFieldConfig extends Serializable {
    List<ReturnField> getAllFields();

    List<ReturnField> getReturnFields();

    List<ReturnField> getDefaultReturnFields();

    List<ReturnField> getRequiredReturnFields();

    ReturnField getReturnFieldByName(String fieldName);

    boolean returnFieldExists(String fieldName);
}
