package org.uniprot.store.indexer.crossref.processor;

import org.springframework.batch.item.ItemProcessor;
import org.uniprot.core.Statistics;
import org.uniprot.core.cv.xdb.CrossRefEntry;
import org.uniprot.core.util.Utils;
import org.uniprot.store.search.document.dbxref.CrossRefDocument;

public class CrossRefProcessor implements ItemProcessor<CrossRefEntry, CrossRefDocument> {
    @Override
    public CrossRefDocument process(CrossRefEntry crossRefEntry) throws Exception {
        CrossRefDocument.CrossRefDocumentBuilder builder = CrossRefDocument.builder();
        builder.category(crossRefEntry.getCategory());
        builder.abbrev(crossRefEntry.getAbbrev()).id(crossRefEntry.getId());
        builder.dbUrl(crossRefEntry.getDbUrl()).doiId(crossRefEntry.getDoiId());
        builder.linkType(crossRefEntry.getLinkType()).name(crossRefEntry.getName());
        builder.pubMedId(crossRefEntry.getPubMedId()).servers(crossRefEntry.getServers());
        if (Utils.notNull(crossRefEntry.getStatistics())) {
            Statistics statistics = crossRefEntry.getStatistics();
            builder.reviewedProteinCount(statistics.getReviewedProteinCount());
            builder.unreviewedProteinCount(statistics.getUnreviewedProteinCount());
        }
        return builder.build();
    }
}
