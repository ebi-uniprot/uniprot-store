package org.uniprot.store.config.searchfield.factory;

import org.uniprot.store.config.searchfield.common.SearchFieldConfig;
import org.uniprot.store.config.searchfield.impl.*;

public class SearchFieldConfigFactory {

    public static SearchFieldConfig getSearchFieldConfig(UniProtDataType type) {
        SearchFieldConfig fieldConfiguration;
        switch (type) {
            case crossref:
                fieldConfiguration = CrossRefSearchFieldConfiguration.getInstance();
                break;
            case disease:
                fieldConfiguration = DiseaseSearchFieldConfiguration.getInstance();
                break;
            case genecentric:
                fieldConfiguration = GeneCentricSearchFieldConfiguration.getInstance();
                break;
            case keyword:
                fieldConfiguration = KeywordSearchFieldConfiguration.getInstance();
                break;
            case literature:
                fieldConfiguration = LiteratureSearchFieldConfiguration.getInstance();
                break;
            case proteome:
                fieldConfiguration = ProteomeSearchFieldConfiguration.getInstance();
                break;
            case subcelllocation:
                fieldConfiguration = SubcellLocationSearchFieldConfiguration.getInstance();
                break;
            case suggest:
                fieldConfiguration = SuggestSearchFieldConfiguration.getInstance();
                break;
            case taxonomy:
                fieldConfiguration = TaxonomySearchFieldConfiguration.getInstance();
                break;
            case uniparc:
                fieldConfiguration = UniParcSearchFieldConfiguration.getInstance();
                break;
            case uniprotkb:
                fieldConfiguration = UniProtKBSearchFieldConfiguration.getInstance();
                break;
            case uniref:
                fieldConfiguration = UniRefSearchFieldConfiguration.getInstance();
                break;
            default:
                fieldConfiguration = null;
                break;
        }

        return fieldConfiguration;
    }
}
