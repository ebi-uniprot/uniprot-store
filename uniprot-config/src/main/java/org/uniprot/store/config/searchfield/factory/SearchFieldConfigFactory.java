package org.uniprot.store.config.searchfield.factory;

import lombok.NonNull;

import org.uniprot.store.config.searchfield.common.SearchFieldConfig;
import org.uniprot.store.config.searchfield.impl.*;

public class SearchFieldConfigFactory {

    public static SearchFieldConfig getSearchFieldConfig(@NonNull UniProtDataType type) {
        SearchFieldConfig fieldConfig;
        switch (type) {
            case crossref:
                fieldConfig = CrossRefSearchFieldConfig.getInstance();
                break;
            case disease:
                fieldConfig = DiseaseSearchFieldConfig.getInstance();
                break;
            case genecentric:
                fieldConfig = GeneCentricSearchFieldConfig.getInstance();
                break;
            case keyword:
                fieldConfig = KeywordSearchFieldConfig.getInstance();
                break;
            case literature:
                fieldConfig = LiteratureSearchFieldConfig.getInstance();
                break;
            case proteome:
                fieldConfig = ProteomeSearchFieldConfig.getInstance();
                break;
            case subcelllocation:
                fieldConfig = SubcellLocationSearchFieldConfig.getInstance();
                break;
            case suggest:
                fieldConfig = SuggestSearchFieldConfig.getInstance();
                break;
            case taxonomy:
                fieldConfig = TaxonomySearchFieldConfig.getInstance();
                break;
            case uniparc:
                fieldConfig = UniParcSearchFieldConfig.getInstance();
                break;
            case uniprotkb:
                fieldConfig = UniProtKBSearchFieldConfig.getInstance();
                break;
            case uniref:
                fieldConfig = UniRefSearchFieldConfig.getInstance();
                break;
            default:
                fieldConfig = null;
                break;
        }

        return fieldConfig;
    }
}
