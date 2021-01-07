package org.uniprot.store.config.searchfield.factory;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

import lombok.NonNull;

import org.uniprot.store.config.UniProtDataType;
import org.uniprot.store.config.searchfield.common.SearchFieldConfig;
import org.uniprot.store.config.searchfield.impl.SearchFieldConfigImpl;

public class SearchFieldConfigFactory {
    public static final String CROSSREF_CONFIG_FILE =
            "search-fields-config/crossref-search-fields.json";
    public static final String DISEASE_CONFIG_FILE =
            "search-fields-config/disease-search-fields.json";
    public static final String GENECENTRIC_CONFIG_FILE =
            "search-fields-config/genecentric-search-fields.json";
    public static final String KEYWORD_CONFIG_FILE =
            "search-fields-config/keyword-search-fields.json";
    public static final String LITERATURE_CONFIG_FILE =
            "search-fields-config/literature-search-fields.json";
    public static final String PROTEOME_CONFIG_FILE =
            "search-fields-config/proteome-search-fields.json";
    public static final String SUBCELLLOCATION_CONFIG_FILE =
            "search-fields-config/subcelllocation-search-fields.json";
    public static final String SUGGEST_CONFIG_FILE =
            "search-fields-config/suggest-search-fields.json";
    public static final String TAXONOMY_CONFIG_FILE =
            "search-fields-config/taxonomy-search-fields.json";
    public static final String UNIPARC_CONFIG_FILE =
            "search-fields-config/uniparc-search-fields.json";
    public static final String UNIPROTKB_CONFIG_FILE =
            "search-fields-config/uniprotkb-search-fields.json";
    public static final String UNIREF_CONFIG_FILE =
            "search-fields-config/uniref-search-fields.json";
    public static final String UNIRULE_CONFIG_FILE =
            "search-fields-config/unirule-search-fields.json";
    public static final String PUBLICATION_CONFIG_FILE =
            "search-fields-config/publication-search-fields.json";

    private static final Map<UniProtDataType, SearchFieldConfig> TYPE_SEARCHFIELDCONFIG_MAP =
            new EnumMap<>(UniProtDataType.class);
    private static final Map<UniProtDataType, String> TYPE_CONFIGFILE_MAP;

    static {
        Map<UniProtDataType, String> typeConfigMap = new EnumMap<>(UniProtDataType.class);
        typeConfigMap.put(UniProtDataType.CROSSREF, CROSSREF_CONFIG_FILE);
        typeConfigMap.put(UniProtDataType.DISEASE, DISEASE_CONFIG_FILE);
        typeConfigMap.put(UniProtDataType.GENECENTRIC, GENECENTRIC_CONFIG_FILE);
        typeConfigMap.put(UniProtDataType.KEYWORD, KEYWORD_CONFIG_FILE);
        typeConfigMap.put(UniProtDataType.LITERATURE, LITERATURE_CONFIG_FILE);
        typeConfigMap.put(UniProtDataType.PROTEOME, PROTEOME_CONFIG_FILE);
        typeConfigMap.put(UniProtDataType.SUBCELLLOCATION, SUBCELLLOCATION_CONFIG_FILE);
        typeConfigMap.put(UniProtDataType.SUGGEST, SUGGEST_CONFIG_FILE);
        typeConfigMap.put(UniProtDataType.TAXONOMY, TAXONOMY_CONFIG_FILE);
        typeConfigMap.put(UniProtDataType.UNIPARC, UNIPARC_CONFIG_FILE);
        typeConfigMap.put(UniProtDataType.UNIPROTKB, UNIPROTKB_CONFIG_FILE);
        typeConfigMap.put(UniProtDataType.UNIREF, UNIREF_CONFIG_FILE);
        typeConfigMap.put(UniProtDataType.UNIRULE, UNIRULE_CONFIG_FILE);
        typeConfigMap.put(UniProtDataType.PUBLICATION, PUBLICATION_CONFIG_FILE);
        TYPE_CONFIGFILE_MAP = Collections.unmodifiableMap(typeConfigMap);
    }

    public static SearchFieldConfig getSearchFieldConfig(@NonNull UniProtDataType type) {
        // get from map or create one.
        SearchFieldConfig searchFieldConfig =
                TYPE_SEARCHFIELDCONFIG_MAP.getOrDefault(
                        type, new SearchFieldConfigImpl(type, TYPE_CONFIGFILE_MAP.get(type)));
        // put in map if not present
        TYPE_SEARCHFIELDCONFIG_MAP.putIfAbsent(type, searchFieldConfig);

        return searchFieldConfig;
    }

    private SearchFieldConfigFactory() {}
}
