package org.uniprot.store.config.returnfield.factory;

import lombok.NonNull;
import org.uniprot.store.config.UniProtDataType;
import org.uniprot.store.config.returnfield.config.ReturnFieldConfig;
import org.uniprot.store.config.returnfield.config.impl.UniProtKBReturnFieldConfigImpl;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

public class ReturnFieldConfigFactory {
    private static final String UNIPROTKB_CONFIG_FILE =
            "result-fields-config/uniprotkb-return-fields.json";

    private static final Map<UniProtDataType, ReturnFieldConfig> TYPE_FIELD_CONFIG_MAP =
            new EnumMap<>(UniProtDataType.class);
    private static final Map<UniProtDataType, String> TYPE_CONFIG_FILE_MAP;

    static {
        Map<UniProtDataType, String> typeConfigMap = new EnumMap<>(UniProtDataType.class);
        typeConfigMap.put(UniProtDataType.UNIPROTKB, UNIPROTKB_CONFIG_FILE);
        TYPE_CONFIG_FILE_MAP = Collections.unmodifiableMap(typeConfigMap);
    }

    public static ReturnFieldConfig getReturnFieldConfig(@NonNull UniProtDataType type) {
        return TYPE_FIELD_CONFIG_MAP.computeIfAbsent(
                type,
                dataType -> {
                    // in future, can remove this line when handling return fields from all
                    // UniProtDataTypes.
                    if (!dataType.equals(UniProtDataType.UNIPROTKB)) {
                        throw new IllegalArgumentException("Unsupported type: " + type);
                    }
                    return new UniProtKBReturnFieldConfigImpl(TYPE_CONFIG_FILE_MAP.get(dataType));
                });
    }

    private ReturnFieldConfigFactory() {}
}
