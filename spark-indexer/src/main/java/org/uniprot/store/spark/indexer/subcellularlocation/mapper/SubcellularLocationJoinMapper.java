package org.uniprot.store.spark.indexer.subcellularlocation.mapper;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.uniprot.core.Statistics;
import org.uniprot.core.impl.StatisticsBuilder;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.core.uniprotkb.UniProtKBEntryType;
import org.uniprot.core.uniprotkb.comment.CommentType;
import org.uniprot.core.uniprotkb.comment.SubcellularLocation;
import org.uniprot.core.uniprotkb.comment.SubcellularLocationComment;
import org.uniprot.core.uniprotkb.comment.SubcellularLocationValue;

import java.util.Iterator;
import java.util.List;

import scala.Tuple2;

/**
 * Extracts the subcellular locations from a UniProt entry
 * @author sahmad
 * @created 31/01/2022
 */
public class SubcellularLocationJoinMapper implements PairFlatMapFunction<Tuple2<String, UniProtKBEntry>, String, Statistics> {
    // returns Iterator of Tuple2<SL-xxxx, Tuple2<Accession, isReviewed>>
    @Override
    public Iterator<Tuple2<String, Statistics>> call(Tuple2<String, UniProtKBEntry> accessionEntry) throws Exception {
        UniProtKBEntry entry = accessionEntry._2;
        List<SubcellularLocationComment> comments = entry.getCommentsByType(CommentType.SUBCELLULAR_LOCATION);
        Statistics statistics = getStatistics(entry);
        return comments.stream().flatMap(comment -> comment.getSubcellularLocations().stream())
                .filter(SubcellularLocation::hasLocation)
                .map(SubcellularLocation::getLocation)
                .map(SubcellularLocationValue::getId)
                .map(subcellId -> new Tuple2<>(subcellId, statistics))
                .iterator();
    }

    private Statistics getStatistics(UniProtKBEntry entry) {
        StatisticsBuilder statisticsBuilder = new StatisticsBuilder();
        if(!isIsoform(entry.getPrimaryAccession().getValue())) {
            boolean isReviewed = entry.getEntryType() == UniProtKBEntryType.SWISSPROT;
            statisticsBuilder = isReviewed ? statisticsBuilder.reviewedProteinCount(1L)
                    : statisticsBuilder.unreviewedProteinCount(1L);
        }

        return statisticsBuilder.build();
    }

    private boolean isIsoform(String accession) {
        return accession.contains("-");
    }
}
