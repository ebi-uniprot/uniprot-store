package org.uniprot.store.indexer.genecentric;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.SolrQuery;
import org.springframework.batch.item.ItemProcessor;
import org.uniprot.core.genecentric.GeneCentricEntry;
import org.uniprot.core.genecentric.Protein;
import org.uniprot.core.genecentric.impl.GeneCentricEntryBuilder;
import org.uniprot.core.genecentric.impl.ProteinBuilder;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.genecentric.GeneCentricDocument;
import org.uniprot.store.search.document.genecentric.GeneCentricDocumentConverter;

/**
 * @author lgonzales
 * @since 04/11/2020
 */
public class GeneCentricRelatedProcessor
        implements ItemProcessor<GeneCentricEntry, GeneCentricDocument> {

    private final UniProtSolrClient uniProtSolrClient;
    private final GeneCentricDocumentConverter converter;

    public GeneCentricRelatedProcessor(
            UniProtSolrClient uniProtSolrClient,
            GeneCentricDocumentConverter geneCentricDocumentConverter) {
        this.uniProtSolrClient = uniProtSolrClient;
        this.converter = geneCentricDocumentConverter;
    }

    @Override
    public GeneCentricDocument process(GeneCentricEntry geneCentricEntry) throws Exception {
        // Related protein names contains the prefix: Isoform of P0CX05,
        Protein relatedProtein = geneCentricEntry.getCanonicalProtein();
        String[] splitProteinName = relatedProtein.getProteinName().split(",");

        String prefix = splitProteinName[0];
        String canonicalAccession = prefix.substring(prefix.lastIndexOf(" ") + 1);

        if (canonicalAccession.equalsIgnoreCase("readthrough")) {
            return null; // skip readthrough
        } else {
            relatedProtein = getRelatedProteinWithoutPrefix(relatedProtein, splitProteinName);

            GeneCentricEntry canonical = getCanonicalEntryFromSolr(canonicalAccession);
            GeneCentricEntryBuilder builder = GeneCentricEntryBuilder.from(canonical);
            builder.relatedProteinsAdd(relatedProtein);
            return converter.convert(builder.build());
        }
    }

    private GeneCentricEntry getCanonicalEntryFromSolr(String canonicalId) {
        SolrQuery query = new SolrQuery("accession:" + canonicalId);
        Optional<GeneCentricDocument> optionalDocument =
                uniProtSolrClient.queryForObject(
                        SolrCollection.genecentric, query, GeneCentricDocument.class);

        return optionalDocument
                .map(converter::getCanonicalEntryFromDocument)
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        "Unable to find canonical entry for id" + canonicalId));
    }

    private Protein getRelatedProteinWithoutPrefix(
            Protein fastaProtein, String[] splitProteinName) {
        String proteinName = Arrays.stream(splitProteinName).skip(1).collect(Collectors.joining());
        return ProteinBuilder.from(fastaProtein).proteinName(proteinName.trim()).build();
    }
}
