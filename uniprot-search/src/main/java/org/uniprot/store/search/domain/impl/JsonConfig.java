package org.uniprot.store.search.domain.impl;

import org.uniprot.store.search.domain.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

public class JsonConfig {

    public static ObjectMapper getJsonMapper() {
        final ObjectMapper objectMapper = new ObjectMapper();
        SimpleModule mod = new SimpleModule();
        mod.addAbstractTypeMapping(EvidenceGroup.class, EvidenceGroupImpl.class);
        mod.addAbstractTypeMapping(EvidenceItem.class, EvidenceItemImpl.class);
        mod.addAbstractTypeMapping(FieldGroup.class, FieldGroupImpl.class);
        mod.addAbstractTypeMapping(Field.class, FieldImpl.class);
        mod.addAbstractTypeMapping(Tuple.class, TupleImpl.class);
        objectMapper.registerModule(mod);
        return objectMapper;
    }
}
