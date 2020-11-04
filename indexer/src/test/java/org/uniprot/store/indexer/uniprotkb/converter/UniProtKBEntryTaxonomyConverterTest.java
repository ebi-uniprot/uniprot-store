package org.uniprot.store.indexer.uniprotkb.converter;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.*;

import org.junit.jupiter.api.Test;
import org.uniprot.core.uniprotkb.taxonomy.Organism;
import org.uniprot.core.uniprotkb.taxonomy.OrganismHost;
import org.uniprot.core.uniprotkb.taxonomy.impl.OrganismBuilder;
import org.uniprot.core.uniprotkb.taxonomy.impl.OrganismHostBuilder;
import org.uniprot.cv.taxonomy.TaxonomicNode;
import org.uniprot.cv.taxonomy.TaxonomyRepo;
import org.uniprot.store.search.document.suggest.SuggestDocument;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

/**
 * @author lgonzales
 * @since 2019-09-06
 */
class UniProtKBEntryTaxonomyConverterTest {

    @Test
    void convertModelOrganism() {
        // given
        TaxonomicNode parentNode =
                getTaxonomyNode(
                        9605,
                        "scientific parent",
                        "common parent",
                        "synonym parent",
                        "mnemonic parent",
                        null);
        TaxonomicNode taxonomicNode =
                getTaxonomyNode(9606, "Homo sapiens", "Human", "Homo sapian", "HUMAN", parentNode);
        Organism organism = new OrganismBuilder().taxonId(9606L).build();

        // objects that will be updated in the conversion method
        UniProtDocument uniProtDocument = new UniProtDocument();
        Map<String, SuggestDocument> suggestions = new HashMap<>();

        TaxonomyRepo repo = mock(TaxonomyRepo.class);
        when(repo.retrieveNodeUsingTaxID(9606)).thenReturn(Optional.of(taxonomicNode));
        when(repo.retrieveNodeUsingTaxID(9605)).thenReturn(Optional.of(parentNode));

        UniProtEntryTaxonomyConverter converter =
                new UniProtEntryTaxonomyConverter(repo, suggestions);

        // when
        converter.convertOrganism(organism, uniProtDocument);

        // then
        assertEquals(9606, uniProtDocument.organismTaxId);

        // organism fields
        assertEquals(asList("Homo sapiens", "Human", "Homo sapian"), uniProtDocument.organismName);
        assertEquals(30, uniProtDocument.organismSort.length());
        assertEquals("Homo sapiens Human Homo sapian", uniProtDocument.organismSort);

        // organism facet fields
        assertEquals("Human", uniProtDocument.modelOrganism);
        assertNull(uniProtDocument.otherOrganism);

        // lineage fields
        assertEquals(asList(9606, 9605), uniProtDocument.taxLineageIds);
        assertEquals(
                asList(
                        "Homo sapiens",
                        "Human",
                        "Homo sapian",
                        "scientific parent",
                        "common parent",
                        "synonym parent"),
                uniProtDocument.organismTaxon);

        // suggestion documents for organism and taxonomy lineage...
        assertEquals(3, suggestions.size());
        assertTrue(suggestions.containsKey("ORGANISM:9606"));

        SuggestDocument suggestionDocument = suggestions.get("ORGANISM:9606");
        assertEquals(suggestionDocument.id, "9606");
        assertEquals(suggestionDocument.value, "Homo sapiens");
        assertEquals(suggestionDocument.altValues, asList("Human", "Homo sapian", "HUMAN"));
        assertEquals(suggestionDocument.dictionary, "ORGANISM");
        assertEquals(suggestionDocument.importance, "medium");

        assertTrue(suggestions.containsKey("TAXONOMY:9606"));
        assertTrue(suggestions.containsKey("TAXONOMY:9605"));
    }

    @Test
    void convertOtherOrganismReplacingNullSuggestionValues() {
        // given
        TaxonomicNode taxonomicNode =
                getTaxonomyNode(9000, "other scientific", null, "other synonym", null, null);
        Organism organism = new OrganismBuilder().taxonId(9000L).build();

        // objects that will be updated in the convertion method
        UniProtDocument uniProtDocument = new UniProtDocument();
        Map<String, SuggestDocument> suggestions = new HashMap<>();

        // adding a null suggestion so It can be replaced
        suggestions.put(
                "ORGANISM:9000",
                SuggestDocument.builder().id("9000").dictionary("ORGANISM").build());

        TaxonomyRepo repo = mock(TaxonomyRepo.class);
        when(repo.retrieveNodeUsingTaxID(9000)).thenReturn(Optional.of(taxonomicNode));

        UniProtEntryTaxonomyConverter converter =
                new UniProtEntryTaxonomyConverter(repo, suggestions);

        // when
        converter.convertOrganism(organism, uniProtDocument);

        // then
        assertEquals(9000, uniProtDocument.organismTaxId);

        // organism fields
        assertEquals(asList("other scientific", "other synonym"), uniProtDocument.organismName);
        assertEquals("other scientific other synonym", uniProtDocument.organismSort);

        // organism facet fields
        assertEquals("other scientific", uniProtDocument.otherOrganism);
        assertNull(uniProtDocument.modelOrganism);

        // lineage fields
        assertEquals(asList(9000), uniProtDocument.taxLineageIds);
        assertEquals(asList("other scientific", "other synonym"), uniProtDocument.organismTaxon);

        // suggestion documents for organism and taxonomy lineage...
        assertEquals(2, suggestions.size());
        assertTrue(suggestions.containsKey("ORGANISM:9000"));

        SuggestDocument suggestionDocument = suggestions.get("ORGANISM:9000");
        assertEquals(suggestionDocument.id, "9000");
        assertEquals(suggestionDocument.value, "other scientific");
        assertEquals(suggestionDocument.altValues, asList("other synonym"));
        assertEquals(suggestionDocument.dictionary, "ORGANISM");
        assertEquals(suggestionDocument.importance, "medium");

        assertTrue(suggestions.containsKey("TAXONOMY:9000"));
    }

    @Test
    void convertSingleOrganismHost() {
        TaxonomicNode organismHostNode =
                getTaxonomyNode(9606, "Homo sapiens", "Human", "Homo sapian", "HUMAN", null);
        OrganismHost organismHost = new OrganismHostBuilder().taxonId(9606L).build();

        // objects that will be updated in the convertion method
        UniProtDocument uniProtDocument = new UniProtDocument();
        Map<String, SuggestDocument> suggestions = new HashMap<>();

        TaxonomyRepo repo = mock(TaxonomyRepo.class);
        when(repo.retrieveNodeUsingTaxID(9606)).thenReturn(Optional.of(organismHostNode));

        UniProtEntryTaxonomyConverter converter =
                new UniProtEntryTaxonomyConverter(repo, suggestions);

        // when
        converter.convertOrganismHosts(singletonList(organismHost), uniProtDocument);

        // then
        assertEquals(singletonList(9606), uniProtDocument.organismHostIds);

        // organism fields
        assertEquals(
                asList("Homo sapiens", "Human", "Homo sapian"), uniProtDocument.organismHostNames);

        // suggestion documents for organism host...
        assertEquals(1, suggestions.size());
        assertTrue(suggestions.containsKey("HOST:9606"));

        SuggestDocument suggestionDocument = suggestions.get("HOST:9606");
        assertEquals(suggestionDocument.id, "9606");
        assertEquals(suggestionDocument.value, "Homo sapiens");
        assertEquals(suggestionDocument.altValues, asList("Human", "Homo sapian", "HUMAN"));
        assertEquals(suggestionDocument.dictionary, "HOST");
        assertEquals(suggestionDocument.importance, "medium");
    }

    @Test
    void convertMultipleOrganismHosts() {
        TaxonomicNode organismHostNode =
                getTaxonomyNode(9606, "Homo sapiens", "Human", "Homo sapian", "HUMAN", null);
        TaxonomicNode otherOrganismHostNode =
                getTaxonomyNode(
                        9000, "other scientific", "other common", "other synonym", "OTHER", null);
        OrganismHost organismHost = new OrganismHostBuilder().taxonId(9606L).build();
        OrganismHost otherOrganismHost = new OrganismHostBuilder().taxonId(9000L).build();

        // objects that will be updated in the convertion method
        UniProtDocument uniProtDocument = new UniProtDocument();
        Map<String, SuggestDocument> suggestions = new HashMap<>();

        TaxonomyRepo repo = mock(TaxonomyRepo.class);
        when(repo.retrieveNodeUsingTaxID(9606)).thenReturn(Optional.of(organismHostNode));
        when(repo.retrieveNodeUsingTaxID(9000)).thenReturn(Optional.of(otherOrganismHostNode));

        UniProtEntryTaxonomyConverter converter =
                new UniProtEntryTaxonomyConverter(repo, suggestions);

        // when
        converter.convertOrganismHosts(asList(organismHost, otherOrganismHost), uniProtDocument);

        // then
        assertEquals(asList(9606, 9000), uniProtDocument.organismHostIds);

        // organism fields
        assertEquals(
                asList(
                        "Homo sapiens",
                        "Human",
                        "Homo sapian",
                        "other scientific",
                        "other common",
                        "other synonym"),
                uniProtDocument.organismHostNames);

        // suggestion documents for organism host...
        assertEquals(2, suggestions.size());
        assertTrue(suggestions.containsKey("HOST:9606"));
        assertTrue(suggestions.containsKey("HOST:9000"));

        SuggestDocument suggestionDocument = suggestions.get("HOST:9606");
        assertEquals(suggestionDocument.id, "9606");
        assertEquals(suggestionDocument.value, "Homo sapiens");
        assertEquals(suggestionDocument.altValues, asList("Human", "Homo sapian", "HUMAN"));
        assertEquals(suggestionDocument.dictionary, "HOST");
        assertEquals(suggestionDocument.importance, "medium");
    }

    private TaxonomicNode getTaxonomyNode(
            int id,
            String scientificName,
            String commonName,
            String synonym,
            String mnemonic,
            TaxonomicNode parent) {
        return new TaxonomicNode() {
            @Override
            public int id() {
                return id;
            }

            @Override
            public String scientificName() {
                return scientificName;
            }

            @Override
            public String commonName() {
                return commonName;
            }

            @Override
            public String synonymName() {
                return synonym;
            }

            @Override
            public String mnemonic() {
                return mnemonic;
            }

            @Override
            public TaxonomicNode parent() {
                return parent;
            }

            @Override
            public boolean hasParent() {
                return parent != null;
            }
        };
    }
}
