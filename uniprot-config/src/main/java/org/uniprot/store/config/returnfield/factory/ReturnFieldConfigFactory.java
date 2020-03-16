package org.uniprot.store.config.returnfield.factory;

import org.uniprot.store.config.UniProtDataType;
import org.uniprot.store.config.returnfield.config.ReturnFieldConfig;
import org.uniprot.store.config.returnfield.config.impl.UniProtKBReturnFieldConfigImpl;
import org.uniprot.store.config.returnfield.config.impl.UniProtReturnFieldConfigImpl;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

public class ReturnFieldConfigFactory {
    private static final String UNIPROTKB_CONFIG_FILE =
            "return-fields-config/uniprotkb-return-fields.json";
    private static final String UNIREF_CONFIG_FILE =
            "return-fields-config/uniref-return-fields.json";
    private static final String UNIPARC_CONFIG_FILE =
            "return-fields-config/uniparc-return-fields.json";

    private static final Map<UniProtDataType, ReturnFieldConfig> TYPE_FIELD_CONFIG_MAP =
            new EnumMap<>(UniProtDataType.class);
    private static final Map<UniProtDataType, String> TYPE_CONFIG_FILE_MAP;

    static {
        Map<UniProtDataType, String> typeConfigMap = new EnumMap<>(UniProtDataType.class);
        typeConfigMap.put(UniProtDataType.UNIPROTKB, UNIPROTKB_CONFIG_FILE);
        typeConfigMap.put(UniProtDataType.UNIREF, UNIREF_CONFIG_FILE);
        typeConfigMap.put(UniProtDataType.UNIPARC, UNIPARC_CONFIG_FILE);
        TYPE_CONFIG_FILE_MAP = Collections.unmodifiableMap(typeConfigMap);
    }

    public static ReturnFieldConfig getReturnFieldConfig(UniProtDataType type) {
        return TYPE_FIELD_CONFIG_MAP.computeIfAbsent(
                type,
                dataType -> {
                    if (!TYPE_CONFIG_FILE_MAP.containsKey(type)) {
                        throw new IllegalArgumentException("Unsupported type: " + type);
                    }
                    
                    String configFile = TYPE_CONFIG_FILE_MAP.get(dataType);
                    if (type.equals(UniProtDataType.UNIPROTKB)) {
                        return new UniProtKBReturnFieldConfigImpl(configFile);
                    } else {
                        return new UniProtReturnFieldConfigImpl(configFile);
                    }
                });
    }

    private ReturnFieldConfigFactory() {}
}
