package org.uniprot.store.search.field;

import org.uniprot.store.search.domain2.SearchField;
import org.uniprot.store.search.domain2.SearchFieldsLoader;
import org.uniprot.store.search.domain2.UniProtKBSearchFieldsLoader;

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
    CROSSREF("crossref"),
    DISEASE("disease"),
    GENECENTRIC("gene-centric"),
    KEYWORD("keyword"),
    LITERATURE("literature"),
    PROTEOME("proteome"),
    SUBCELL("subcell-location"),
    SUGGEST("suggest"),
    TAXONOMY("taxonomy"),
    UNIPARC("uniparc"),
    UNIPROTKB("uniprot"),
    UNIREF("uniref");

    private String configPath;
    private SearchFieldsLoader searchFieldsLoader;

    UniProtSearchFields(String configPath) {
        this.configPath = String.format("%s/%s", configPath, "search-fields.json");
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
