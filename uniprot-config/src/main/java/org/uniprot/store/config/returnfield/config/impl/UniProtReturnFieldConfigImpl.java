package org.uniprot.store.config.returnfield.config.impl;

import static java.util.Collections.emptyList;

import java.util.Collection;

import org.uniprot.store.config.returnfield.config.AbstractReturnFieldConfig;
import org.uniprot.store.config.returnfield.model.ReturnField;

/**
 * General purpose loading of a valid JSON definition of return field definitions, that does not add
 * any dynamic field information.
 *
 * <p>Created 16/03/20
 *
 * @author Edd
 */
public class UniProtReturnFieldConfigImpl extends AbstractReturnFieldConfig {

    private static final long serialVersionUID = 2724872394755111932L;

    public UniProtReturnFieldConfigImpl(String configFile) {
        super(configFile);
    }

    @Override
    protected Collection<ReturnField> dynamicallyLoadFields() {
        return emptyList();
    }
}
