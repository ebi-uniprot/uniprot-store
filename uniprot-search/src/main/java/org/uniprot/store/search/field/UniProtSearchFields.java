package org.uniprot.store.search.field;

import java.util.Objects;
import java.util.Set;

import org.uniprot.store.search.domain2.CrossRefSearchFields;
import org.uniprot.store.search.domain2.SearchField;
import org.uniprot.store.search.domain2.SearchFieldsLoader;
import org.uniprot.store.search.domain2.UniProtKBSearchFields;

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

    @Override
    public SearchField getSortFieldFor(String field) {
        checkInitialised();
        return searchFieldsLoader.getSortFieldFor(field);
    }

    private void checkInitialised() {
        if (Objects.isNull(searchFieldsLoader)) {
            if (configPath.startsWith("uniprot")) {
                searchFieldsLoader = new UniProtKBSearchFields();
            } else if (configPath.startsWith("crossref")) {
                searchFieldsLoader = new CrossRefSearchFields();
            } else {
                searchFieldsLoader = new SearchFieldsLoader(configPath);
            }
        }
    }
}
