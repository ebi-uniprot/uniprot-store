package org.uniprot.store.spark.indexer.proteome.mapper;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.springframework.stereotype.Component;
import org.uniprot.core.proteome.ProteomeStatistics;
import org.uniprot.store.search.document.proteome.ProteomeDocument;
import scala.Tuple2;

@Component
public class StatisticsToProteomeDocumentMapper implements Function<Tuple2<ProteomeDocument, Optional<ProteomeStatistics>>, ProteomeDocument> {

    @Override
    public ProteomeDocument call(Tuple2<ProteomeDocument, Optional<ProteomeStatistics>> proteomeDocumentStatisticsTuple2) throws Exception {
        ProteomeDocument proteomeDocument = proteomeDocumentStatisticsTuple2._1;
        Optional<ProteomeStatistics> proteomeStatisticsOptional = proteomeDocumentStatisticsTuple2._2;

        if (proteomeStatisticsOptional.isPresent()) {
            ProteomeStatistics proteomeStatistics = proteomeStatisticsOptional.get();
            proteomeDocument.reviewedProteinCount = proteomeStatistics.getReviewedProteinCount();
            proteomeDocument.unreviewedProteinCount = proteomeStatistics.getUnreviewedProteinCount();
            proteomeDocument.isoformProteinCount = proteomeStatistics.getIsoformProteinCount();
        }

        return proteomeDocument;
    }
}
