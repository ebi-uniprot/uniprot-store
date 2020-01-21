package org.uniprot.store.search.domain2;

import java.util.Objects;
import java.util.Set;

/**
 * Represents all accessible search fields for UniProt domains (e.g., UniProtKB, UniParc, UniRef),
 * and provides access to them via the {@link SearchFields} contract.
 *
 * <p>Created 17/01/2020
 *
 * @author Edd
 */
public enum UniProtSearchFields implements SearchFields {
    CROSSREF("crossref/search-fields.json"),
    DISEASE("disease/search-fields.json"),
    GENECENTRIC("gene-centric/search-fields.json"),
    KEYWORD("keyword/search-fields.json"),
    LITERATURE("literature/search-fields.json"),
    PROTEOME("proteome/search-fields.json"),
    SUBCELL("subcell-location/search-fields.json"),
    TAXONOMY("taxonomy/search-fields.json"),
    UNIPARC("uniparc/search-fields.json"),
    UNIPROTKB("uniprot/search-fields.json"),
    UNIREF("uniref/search-fields.json");

    private String configPath;
    private SearchFieldsLoader searchFieldsLoader;

    UniProtSearchFields(String configPath) {
        this.configPath = configPath;
    }

    @Override
    public Set<SearchField> getSearchFields() {
        checkInitialised();
        return searchFieldsLoader.getSearchFields();
    }

    @Override
    public Set<SearchField> getSortFields() {
        checkInitialised();
        return searchFieldsLoader.getSortFields();
    }

    private void checkInitialised() {
        if (Objects.isNull(searchFieldsLoader)) {
            if (configPath.startsWith("uniprot")) {
                searchFieldsLoader = new UniProtKBSearchFieldsLoader(configPath);
            } else {
                searchFieldsLoader = new SearchFieldsLoader(configPath);
            }
        }
    }
}
