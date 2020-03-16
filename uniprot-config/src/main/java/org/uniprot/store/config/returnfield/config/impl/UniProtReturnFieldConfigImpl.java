package org.uniprot.store.config.returnfield.config.impl;

import org.uniprot.store.config.returnfield.config.AbstractReturnFieldConfig;
import org.uniprot.store.config.returnfield.model.ReturnField;

import java.util.Collection;

import static java.util.Collections.emptyList;

/**
 * General purpose loading of a valid JSON definition of return field definitions, that does not add
 * any dynamic field information.
 *
 * <p>Created 16/03/20
 *
 * @author Edd
 */
public class UniProtReturnFieldConfigImpl extends AbstractReturnFieldConfig {
    public UniProtReturnFieldConfigImpl(String configFile) {
        super(configFile);
    }

    @Override
    protected Collection<ReturnField> dynamicallyLoadFields() {
        return emptyList();
    }
}
