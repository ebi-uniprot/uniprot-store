package org.uniprot.store.config.returnfield.factory;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

import org.uniprot.store.config.UniProtDataType;
import org.uniprot.store.config.returnfield.config.ReturnFieldConfig;
import org.uniprot.store.config.returnfield.config.impl.UniProtKBReturnFieldConfigImpl;
import org.uniprot.store.config.returnfield.config.impl.UniProtReturnFieldConfigImpl;

public class ReturnFieldConfigFactory {

    public static final String CROSS_REF_CONFIG_FILE =
            "return-fields-config/crossref-return-fields.json";
    public static final String GENE_CONFIG_FILE =
            "return-fields-config/genecentric-return-fields.json";
    public static final String LITERATURE_CONFIG_FILE =
            "return-fields-config/literature-return-fields.json";
    public static final String SUGGEST_CONFIG_FILE =
            "return-fields-config/suggest-return-fields.json";
    public static final String DISEASE_CONFIG_FILE =
            "return-fields-config/disease-return-fields.json";
    public static final String KEYWORD_CONFIG_FILE =
            "return-fields-config/keyword-return-fields.json";
    public static final String PIR_ID_MAPPING_CONFIG_FILE =
            "return-fields-config/pir-id-mapping-return-fields.json";
    public static final String PROTEOME_CONFIG_FILE =
            "return-fields-config/proteome-return-fields.json";
    public static final String SUBCELL_CONFIG_FILE =
            "return-fields-config/subcelllocation-return-fields.json";
    public static final String TAXONOMY_CONFIG_FILE =
            "return-fields-config/taxonomy-return-fields.json";
    public static final String UNIPROTKB_CONFIG_FILE =
            "return-fields-config/uniprotkb-return-fields.json";
    public static final String UNIREF_CONFIG_FILE =
            "return-fields-config/uniref-return-fields.json";
    public static final String UNIPARC_CONFIG_FILE =
            "return-fields-config/uniparc-return-fields.json";
    public static final String UNIRULE_CONFIG_FILE =
            "return-fields-config/unirule-return-fields.json";
    public static final String UNIPARC_CROSSREF_CONFIG_FILE =
            "return-fields-config/uniparc-crossref-return-fields.json";
    public static final String HELP_CONFIG_FILE = "return-fields-config/help-return-fields.json";
    public static final String ARBA_CONFIG_FILE = "return-fields-config/arba-return-fields.json";

    private static final Map<UniProtDataType, ReturnFieldConfig> TYPE_FIELD_CONFIG_MAP =
            new EnumMap<>(UniProtDataType.class);
    private static final Map<UniProtDataType, String> TYPE_CONFIG_FILE_MAP;

    static {
        Map<UniProtDataType, String> typeConfigMap = new EnumMap<>(UniProtDataType.class);
        typeConfigMap.put(UniProtDataType.CROSSREF, CROSS_REF_CONFIG_FILE);
        typeConfigMap.put(UniProtDataType.GENECENTRIC, GENE_CONFIG_FILE);
        typeConfigMap.put(UniProtDataType.LITERATURE, LITERATURE_CONFIG_FILE);
        typeConfigMap.put(UniProtDataType.SUGGEST, SUGGEST_CONFIG_FILE);
        typeConfigMap.put(UniProtDataType.DISEASE, DISEASE_CONFIG_FILE);
        typeConfigMap.put(UniProtDataType.KEYWORD, KEYWORD_CONFIG_FILE);
        typeConfigMap.put(UniProtDataType.PIR_ID_MAPPING, PIR_ID_MAPPING_CONFIG_FILE);
        typeConfigMap.put(UniProtDataType.PROTEOME, PROTEOME_CONFIG_FILE);
        typeConfigMap.put(UniProtDataType.SUBCELLLOCATION, SUBCELL_CONFIG_FILE);
        typeConfigMap.put(UniProtDataType.TAXONOMY, TAXONOMY_CONFIG_FILE);
        typeConfigMap.put(UniProtDataType.UNIPROTKB, UNIPROTKB_CONFIG_FILE);
        typeConfigMap.put(UniProtDataType.UNIREF, UNIREF_CONFIG_FILE);
        typeConfigMap.put(UniProtDataType.UNIPARC, UNIPARC_CONFIG_FILE);
        typeConfigMap.put(UniProtDataType.UNIRULE, UNIRULE_CONFIG_FILE);
        typeConfigMap.put(UniProtDataType.UNIPARC_CROSSREF, UNIPARC_CROSSREF_CONFIG_FILE);
        typeConfigMap.put(UniProtDataType.HELP, HELP_CONFIG_FILE);
        typeConfigMap.put(UniProtDataType.ARBA, ARBA_CONFIG_FILE);
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
