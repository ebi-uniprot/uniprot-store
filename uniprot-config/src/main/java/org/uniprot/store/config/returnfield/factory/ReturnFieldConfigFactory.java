package org.uniprot.store.config.returnfield.factory;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

import lombok.NonNull;

import org.uniprot.store.config.UniProtDataType;
import org.uniprot.store.config.returnfield.common.ReturnFieldConfig;
import org.uniprot.store.config.returnfield.common.ReturnFieldConfigImpl;

public class ReturnFieldConfigFactory {
    public static final String UNIPROTKB_CONFIG_FILE =
            "result-fields-config/uniprotkb-result-fields.json";
    public static final String UNIREF_CONFIG_FILE =
            "result-fields-config/uniref-return-fields.json";
    public static final String UNIPARC_CONFIG_FILE =
            "result-fields-config/uniparc-return-fields.json";

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

    public static ReturnFieldConfig getReturnFieldConfig(@NonNull UniProtDataType type) {
        return TYPE_FIELD_CONFIG_MAP.computeIfAbsent(
                type,
                t -> {
                    if (!TYPE_CONFIG_FILE_MAP.containsKey(t)) {
                        throw new IllegalArgumentException("Unsupported type: " + type);
                    }
                    return new ReturnFieldConfigImpl(TYPE_CONFIG_FILE_MAP.get(t));
                });
    }

    private ReturnFieldConfigFactory() {}
}
