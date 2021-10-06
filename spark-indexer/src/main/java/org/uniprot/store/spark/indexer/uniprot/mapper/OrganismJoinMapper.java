package org.uniprot.store.spark.indexer.uniprot.mapper;

import java.util.Arrays;

import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.core.flatfile.parser.UniprotKBLineParser;
import org.uniprot.core.flatfile.parser.impl.DefaultUniprotKBLineParserFactory;
import org.uniprot.core.flatfile.parser.impl.ox.OxLineObject;
import org.uniprot.core.taxonomy.TaxonomyStatistics;
import org.uniprot.core.taxonomy.impl.TaxonomyStatisticsBuilder;
import org.uniprot.store.spark.indexer.common.exception.SparkIndexException;

import scala.Tuple2;

public class OrganismJoinMapper implements PairFunction<String, String, TaxonomyStatistics> {

    private static final long serialVersionUID = -7523338650713478372L;

    @Override
    public Tuple2<String, TaxonomyStatistics> call(String entryStr) throws Exception {
        final UniprotKBLineParser<OxLineObject> oxParser =
                new DefaultUniprotKBLineParserFactory().createOxLineParser();
        SparkIndexException exception =
                new SparkIndexException("Unable to map OX line for entry:" + entryStr);
        String[] lines = entryStr.split("\n");

        String oxLine =
                Arrays.stream(lines)
                        .filter(line -> line.startsWith("OX  "))
                        .findFirst()
                        .orElseThrow(() -> exception);
        int organismId = oxParser.parse(oxLine + "\n").taxonomy_id;
        // TODO: Proteome counts
        TaxonomyStatisticsBuilder statisticsBuilder = new TaxonomyStatisticsBuilder();
        if (lines[0].contains("Reviewed;")) {
            statisticsBuilder.reviewedProteinCount(1L);
        } else {
            statisticsBuilder.unreviewedProteinCount(1L);
        }
        return new Tuple2<>(String.valueOf(organismId), statisticsBuilder.build());
    }
}
