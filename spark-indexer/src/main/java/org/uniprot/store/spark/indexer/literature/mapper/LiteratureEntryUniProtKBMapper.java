package org.uniprot.store.spark.indexer.literature.mapper;

import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.uniprot.core.citation.Citation;
import org.uniprot.core.literature.LiteratureEntry;
import org.uniprot.core.literature.LiteratureStatistics;
import org.uniprot.core.literature.impl.LiteratureEntryBuilder;
import org.uniprot.core.literature.impl.LiteratureStatisticsBuilder;
import org.uniprot.core.uniprotkb.UniProtKBReference;
import org.uniprot.store.spark.indexer.common.converter.UniProtKBReferencesConverter;

import scala.Tuple2;

/**
 * Maps an entry string to an iterator of tuples with values <citationId, LiteratureEntry>.
 *
 * @author lgonzales
 * @since 24/03/2021
 */
public class LiteratureEntryUniProtKBMapper
        implements PairFlatMapFunction<String, String, LiteratureEntry> {

    private static final Pattern REVIEWED_REGEX = Pattern.compile("^ID.+Reviewed.+$");
    private static final long serialVersionUID = -3292552212361568704L;
    private final UniProtKBReferencesConverter uniProtKBReferencesConverter =
            new UniProtKBReferencesConverter();

    @Override
    public Iterator<Tuple2<String, LiteratureEntry>> call(String entryStr) throws Exception {
        String[] lines = entryStr.split("\n");
        LiteratureStatistics statistics = getLiteratureStatistics(lines);
        List<UniProtKBReference> references = uniProtKBReferencesConverter.convert(lines);
        return references.stream()
                .filter(UniProtKBReference::hasCitation)
                .map(ref -> buildEntry(ref.getCitation(), statistics))
                .map(entry -> new Tuple2<>(entry.getCitation().getId(), entry))
                .iterator();
    }

    private LiteratureEntry buildEntry(Citation citation, LiteratureStatistics statistics) {
        return new LiteratureEntryBuilder().citation(citation).statistics(statistics).build();
    }

    private LiteratureStatistics getLiteratureStatistics(String[] lines) {
        LiteratureStatisticsBuilder statsBuilder = new LiteratureStatisticsBuilder();
        if (REVIEWED_REGEX.matcher(lines[0]).matches()) {
            statsBuilder = statsBuilder.reviewedProteinCount(1L);
        } else {
            statsBuilder = statsBuilder.unreviewedProteinCount(1L);
        }
        return statsBuilder.build();
    }
}
