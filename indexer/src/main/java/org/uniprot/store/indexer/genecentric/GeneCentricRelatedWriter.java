package org.uniprot.store.indexer.genecentric;

import java.util.*;
import java.util.stream.Collectors;

import org.springframework.batch.item.ItemWriter;
import org.uniprot.core.genecentric.GeneCentricEntry;
import org.uniprot.core.genecentric.impl.GeneCentricEntryBuilder;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.DocumentConversionException;
import org.uniprot.store.search.document.genecentric.GeneCentricDocument;
import org.uniprot.store.search.document.genecentric.GeneCentricDocumentConverter;

/**
 * @author lgonzales
 * @since 04/11/2020
 */
public class GeneCentricRelatedWriter implements ItemWriter<GeneCentricDocument> {
    private final UniProtSolrClient uniProtSolrClient;
    private final SolrCollection collection;
    private final GeneCentricDocumentConverter converter;

    public GeneCentricRelatedWriter(
            UniProtSolrClient uniProtSolrClient,
            GeneCentricDocumentConverter geneCentricDocumentConverter) {
        this.uniProtSolrClient = uniProtSolrClient;
        this.collection = SolrCollection.genecentric;
        this.converter = geneCentricDocumentConverter;
    }

    @Override
    public void write(List<? extends GeneCentricDocument> documents) {
        Map<String, ? extends List<? extends GeneCentricDocument>> groupedDocuments =
                documents.stream()
                        .collect(Collectors.groupingBy(GeneCentricDocument::getAccession));

        documents =
                groupedDocuments.entrySet().stream()
                        .map(this::groupRelated)
                        .collect(Collectors.toList());

        this.uniProtSolrClient.saveBeans(collection, documents);
        this.uniProtSolrClient.softCommit(collection);
    }

    private GeneCentricDocument groupRelated(
            Map.Entry<String, ? extends List<? extends GeneCentricDocument>> entry) {
        return entry.getValue().stream()
                .map(converter::getCanonicalEntryFromDocument)
                .reduce(this::joinRelated)
                .map(converter::convert)
                .orElseThrow(
                        () ->
                                new DocumentConversionException(
                                        "Unable group related proteins for: " + entry.getKey()));
    }

    private GeneCentricEntry joinRelated(
            GeneCentricEntry geneCentricEntry, GeneCentricEntry otherCentricEntry) {
        GeneCentricEntryBuilder builder = GeneCentricEntryBuilder.from(geneCentricEntry);
        otherCentricEntry.getRelatedProteins().forEach(builder::relatedProteinsAdd);
        return builder.build();
    }
}
