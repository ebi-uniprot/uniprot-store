package org.uniprot.store.spark.indexer.taxonomy;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.uniprot.core.json.parser.taxonomy.TaxonomyJsonConfig;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.TaxonomyStatistics;
import org.uniprot.core.taxonomy.impl.TaxonomyEntryBuilder;
import org.uniprot.store.search.document.taxonomy.TaxonomyDocument;
import org.uniprot.store.search.document.taxonomy.TaxonomyDocumentConverter;
import org.uniprot.store.spark.indexer.taxonomy.mapper.model.TaxonomyStatisticsWrapper;

import com.fasterxml.jackson.databind.ObjectMapper;

import scala.Tuple2;

public class TaxonomyEntryToDocumentMapper
        implements Function<
                Tuple2<TaxonomyEntry, Optional<TaxonomyStatisticsWrapper>>, TaxonomyDocument> {
    private static final long serialVersionUID = 1698828648779824974L;
    private final ObjectMapper objectMapper;

    public TaxonomyEntryToDocumentMapper() {
        objectMapper = TaxonomyJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public TaxonomyDocument call(Tuple2<TaxonomyEntry, Optional<TaxonomyStatisticsWrapper>> tuple2)
            throws Exception {
        TaxonomyEntry entry = tuple2._1;
        if (entry.getTaxonId() == 1L) {
            return null;
        }
        List<String> taxonomiesWith = new ArrayList<>();
        if (tuple2._2.isPresent()) {
            TaxonomyStatisticsWrapper statisticsWrapper = tuple2._2.get();

            TaxonomyStatistics statistics = statisticsWrapper.getStatistics();
            if (statisticsWrapper.isOrganismReviewedProtein()) {
                taxonomiesWith.add("1_uniprotkb");
                taxonomiesWith.add("2_reviewed");
            }
            if (statisticsWrapper.isOrganismUnreviewedProtein()) {
                taxonomiesWith.add("3_unreviewed");
                if (!statisticsWrapper.isOrganismReviewedProtein()) {
                    taxonomiesWith.add("1_uniprotkb");
                }
            }
            if (statistics.hasReferenceProteomeCount()) {
                taxonomiesWith.add("4_reference");
                taxonomiesWith.add("5_proteome");
            }
            if (statistics.hasProteomeCount() && !statistics.hasReferenceProteomeCount()) {
                taxonomiesWith.add("5_proteome");
            }

            TaxonomyEntryBuilder entryBuilder = TaxonomyEntryBuilder.from(entry);
            entryBuilder.statistics(statistics);
            entry = entryBuilder.build();
        }

        TaxonomyDocumentConverter converter = new TaxonomyDocumentConverter(objectMapper);
        TaxonomyDocument.TaxonomyDocumentBuilder docBuilder = converter.convert(entry);
        docBuilder.taxonomiesWith(taxonomiesWith);

        return docBuilder.build();
    }
}
