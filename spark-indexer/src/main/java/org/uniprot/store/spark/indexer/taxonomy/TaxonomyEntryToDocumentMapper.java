package org.uniprot.store.spark.indexer.taxonomy;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.uniprot.core.json.parser.taxonomy.TaxonomyJsonConfig;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.TaxonomyStatistics;
import org.uniprot.core.taxonomy.impl.TaxonomyEntryBuilder;
import org.uniprot.core.taxonomy.impl.TaxonomyStatisticsBuilder;
import org.uniprot.store.search.document.taxonomy.TaxonomyDocument;
import org.uniprot.store.search.document.taxonomy.TaxonomyDocumentConverter;

import scala.Tuple2;

import com.fasterxml.jackson.databind.ObjectMapper;

public class TaxonomyEntryToDocumentMapper
        implements Function<
                Tuple2<
                        Tuple2<TaxonomyEntry, Optional<TaxonomyStatistics>>,
                        Optional<TaxonomyStatistics>>,
                TaxonomyDocument> {
    private static final long serialVersionUID = 1698828648779824974L;
    private final ObjectMapper objectMapper;

    public TaxonomyEntryToDocumentMapper() {
        objectMapper = TaxonomyJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public TaxonomyDocument call(
            Tuple2<
                            Tuple2<TaxonomyEntry, Optional<TaxonomyStatistics>>,
                            Optional<TaxonomyStatistics>>
                    tuple)
            throws Exception {
        TaxonomyDocumentConverter converter = new TaxonomyDocumentConverter(objectMapper);
        TaxonomyEntryBuilder entryBuilder = TaxonomyEntryBuilder.from(tuple._1._1);
        TaxonomyStatisticsBuilder statisticsBuilder = new TaxonomyStatisticsBuilder();
        if (tuple._1._2.isPresent()) {
            TaxonomyStatistics proteinStats = tuple._1._2.get();
            statisticsBuilder.reviewedProteinCount(proteinStats.getReviewedProteinCount());
            statisticsBuilder.unreviewedProteinCount(proteinStats.getUnreviewedProteinCount());
        }
        if (tuple._2.isPresent()) {
            TaxonomyStatistics proteomeStats = tuple._2.get();
            statisticsBuilder.referenceProteomeCount(proteomeStats.getReferenceProteomeCount());
            statisticsBuilder.proteomeCount(proteomeStats.getProteomeCount());
        }
        entryBuilder.statistics(statisticsBuilder.build());
        return converter.convert(entryBuilder.build());
    }
}
