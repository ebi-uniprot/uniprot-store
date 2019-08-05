package org.uniprot.store.indexer.crossref.processor;

import org.springframework.batch.item.ItemProcessor;
import org.uniprot.core.crossref.CrossRefEntry;
import org.uniprot.store.search.document.dbxref.CrossRefDocument;

public class CrossRefProcessor implements ItemProcessor<CrossRefEntry, CrossRefDocument> {
    @Override
    public CrossRefDocument process(CrossRefEntry crossRefEntry) throws Exception {
        CrossRefDocument.CrossRefDocumentBuilder builder = CrossRefDocument.builder();
        builder.category(crossRefEntry.getCategory());
        builder.abbrev(crossRefEntry.getAbbrev()).accession(crossRefEntry.getAccession());
        builder.dbUrl(crossRefEntry.getDbUrl()).doiId(crossRefEntry.getDoiId());
        builder.linkType(crossRefEntry.getLinkType()).name(crossRefEntry.getName());
        builder.pubMedId(crossRefEntry.getPubMedId()).server(crossRefEntry.getServer());
        builder.reviewedProteinCount(crossRefEntry.getReviewedProteinCount());
        builder.unreviewedProteinCount(crossRefEntry.getUnreviewedProteinCount());
        return builder.build();
    }
}
