package org.uniprot.store.config.returnfield.config.impl;

import org.uniprot.store.config.returnfield.config.AbstractReturnFieldConfig;
import org.uniprot.store.config.returnfield.model.ReturnField;

import java.util.Collection;
import java.util.Collections;

/**
 * Created 13/03/20
 *
 * @author Edd
 */
public class UniProtKBReturnFieldConfigImpl extends AbstractReturnFieldConfig {
    public UniProtKBReturnFieldConfigImpl(String configFile) {
        super(configFile);
    }

    @Override
    protected Collection<ReturnField> loadDynamicFields() {
        // iterate through fields
        // if databaseGroup = true
        //     fetch database group from UniProtDatabaseCategory from UniProtDatabaseTypes
        //         for each database inside it, auto generate children dbs
        return Collections.emptyList();
    }
}
