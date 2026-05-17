package org.uniprot.store.indexer.search.precomputed;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.uniprot.store.search.document.precomputed.PrecomputedAnnotationDocument;

class PrecomputedAnnotationSearchIT {
    @RegisterExtension
    static PrecomputedAnnotationSearchEngine searchEngine = new PrecomputedAnnotationSearchEngine();

    @Test
    void canIndexAndSearchByDefaultAccessionField() {
        PrecomputedAnnotationDocument document =
                PrecomputedAnnotationDocument.builder()
                        .accession("P21802")
                        .proteome("UP000005640")
                        .build();

        searchEngine.indexEntry(document);

        QueryResponse response = searchEngine.getQueryResponse("select", "P21802");

        assertThat(response.getResults().getNumFound(), is(1L));
    }

    @Test
    void canSearchProteomeField() {
        PrecomputedAnnotationDocument document =
                PrecomputedAnnotationDocument.builder()
                        .accession("Q9Y261")
                        .proteome("UP000005640")
                        .build();

        searchEngine.indexEntry(document);

        QueryResponse response = searchEngine.getQueryResponse("select", "proteome:UP000005640");

        assertThat(response.getResults().getNumFound(), is(1L));
    }
}
