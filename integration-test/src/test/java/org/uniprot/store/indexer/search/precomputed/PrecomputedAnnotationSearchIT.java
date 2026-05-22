package org.uniprot.store.indexer.search.precomputed;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.List;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.uniprot.store.search.document.precomputed.PrecomputedAnnotationDocument;
import org.uniprot.store.search.field.QueryBuilder;

class PrecomputedAnnotationSearchIT {
    @RegisterExtension
    static PrecomputedAnnotationSearchEngine searchEngine = new PrecomputedAnnotationSearchEngine();

    @Test
    void canIndexAndSearchByDefaultAccessionField() {
        PrecomputedAnnotationDocument document =
                precomputedDocumentBuilder("P21802").proteome("UP000100001").build();

        searchEngine.indexEntry(document);

        QueryResponse response = searchEngine.getQueryResponse("select", "P21802");

        assertThat(response.getResults().getNumFound(), is(1L));
    }

    @Test
    void canSearchProteomeField() {
        String accession = "Q9Y261";
        String proteome = "UP000100002";
        PrecomputedAnnotationDocument document =
                precomputedDocumentBuilder(accession).proteome(proteome).build();

        searchEngine.indexEntry(document);

        QueryResponse response = searchEngine.getQueryResponse("select", proteome(proteome));

        assertThat(response.getResults().getNumFound(), is(1L));
        assertThat(searchEngine.getIdentifiers(response), is(List.of(accession)));
    }

    @Test
    void canSearchProteomeFieldWhenDocumentHasMultipleProteomes() {
        String accession = "A0A0P0PA01";
        String proteome = "UP000100004";
        PrecomputedAnnotationDocument document =
                precomputedDocumentBuilder(accession)
                        .proteome(List.of("UP000100003", "UP000100004"))
                        .build();

        searchEngine.indexEntry(document);

        QueryResponse response = searchEngine.getQueryResponse("select", proteome(proteome));

        assertThat(response.getResults().getNumFound(), is(1L));
        assertThat(searchEngine.getIdentifiers(response), is(List.of(accession)));
    }

    @Test
    void searchByProteomeDoesNotReturnDocumentWithNoProteomes() {
        String matchingAccession = "A0A0P0PA02";
        String emptyProteomeAccession = "A0A0P0PA03";
        String proteome = "UP000100005";
        PrecomputedAnnotationDocument matchingDocument =
                precomputedDocumentBuilder(matchingAccession).proteome(proteome).build();
        PrecomputedAnnotationDocument emptyProteomeDocument =
                precomputedDocumentBuilder(emptyProteomeAccession).build();

        searchEngine.indexEntry(matchingDocument);
        searchEngine.indexEntry(emptyProteomeDocument);

        QueryResponse emptyProteomeResponse =
                searchEngine.getQueryResponse("select", emptyProteomeAccession);
        assertThat(emptyProteomeResponse.getResults().getNumFound(), is(1L));

        QueryResponse response = searchEngine.getQueryResponse("select", proteome(proteome));
        assertThat(response.getResults().getNumFound(), is(1L));
        assertThat(searchEngine.getIdentifiers(response), is(List.of(matchingAccession)));
    }

    @Test
    void canSearchUniparcField() {
        String accession = "A0A0P0PA04";
        String uniparc = "UPI000100006";
        PrecomputedAnnotationDocument document =
                precomputedDocumentBuilder(accession).uniparc(uniparc).build();

        searchEngine.indexEntry(document);

        QueryResponse response = searchEngine.getQueryResponse("select", uniparc(uniparc));

        assertThat(response.getResults().getNumFound(), is(1L));
        assertThat(searchEngine.getIdentifiers(response), is(List.of(accession)));
    }

    @Test
    void searchByUniparcDoesNotReturnNonMatchingDocument() {
        String accession = "A0A0P0PA05";
        PrecomputedAnnotationDocument document =
                precomputedDocumentBuilder(accession).uniparc("UPI000100007").build();

        searchEngine.indexEntry(document);

        QueryResponse response = searchEngine.getQueryResponse("select", uniparc("UPI000100008"));

        assertThat(response.getResults().getNumFound(), is(0L));
    }

    @Test
    void canSearchTaxonomyIdField() {
        String accession = "A0A0P0PA06";
        int taxonomyId = 100100006;
        PrecomputedAnnotationDocument document =
                precomputedDocumentBuilder(accession).taxonomyId(taxonomyId).build();

        searchEngine.indexEntry(document);

        QueryResponse response = searchEngine.getQueryResponse("select", taxonomyId(taxonomyId));

        assertThat(response.getResults().getNumFound(), is(1L));
        assertThat(searchEngine.getIdentifiers(response), is(List.of(accession)));
    }

    @Test
    void searchByTaxonomyIdDoesNotReturnNonMatchingDocument() {
        String accession = "A0A0P0PA07";
        PrecomputedAnnotationDocument document =
                precomputedDocumentBuilder(accession).taxonomyId(100100007).build();

        searchEngine.indexEntry(document);

        QueryResponse response = searchEngine.getQueryResponse("select", taxonomyId(100100008));

        assertThat(response.getResults().getNumFound(), is(0L));
    }

    private PrecomputedAnnotationDocument.PrecomputedAnnotationDocumentBuilder
            precomputedDocumentBuilder(String accession) {
        return PrecomputedAnnotationDocument.builder()
                .accession(accession)
                .taxonomyId(9606)
                .uniparc("UPI" + accession);
    }

    private String proteome(String proteome) {
        return QueryBuilder.query(
                searchEngine
                        .getSearchFieldConfig()
                        .getSearchFieldItemByName("proteome")
                        .getFieldName(),
                proteome);
    }

    private String uniparc(String uniparc) {
        return QueryBuilder.query("uniparc", uniparc);
    }

    private String taxonomyId(int taxonomyId) {
        return QueryBuilder.query("taxonomy_id", "" + taxonomyId);
    }
}
