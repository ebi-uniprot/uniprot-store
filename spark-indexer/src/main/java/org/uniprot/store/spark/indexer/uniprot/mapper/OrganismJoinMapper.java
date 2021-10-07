package org.uniprot.store.spark.indexer.uniprot.mapper;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.core.flatfile.parser.UniprotKBLineParser;
import org.uniprot.core.flatfile.parser.impl.DefaultUniprotKBLineParserFactory;
import org.uniprot.core.flatfile.parser.impl.ac.AcLineObject;
import org.uniprot.core.flatfile.parser.impl.ox.OxLineObject;
import org.uniprot.core.taxonomy.TaxonomyStatistics;
import org.uniprot.core.taxonomy.impl.TaxonomyStatisticsBuilder;

import scala.Tuple2;

public class OrganismJoinMapper implements PairFunction<String, String, TaxonomyStatistics> {

    private static final long serialVersionUID = -7523338650713478372L;

    @Override
    public Tuple2<String, TaxonomyStatistics> call(String entryStr) throws Exception {
        final UniprotKBLineParser<AcLineObject> acParser =
                new DefaultUniprotKBLineParserFactory().createAcLineParser();
        final UniprotKBLineParser<OxLineObject> oxParser =
                new DefaultUniprotKBLineParserFactory().createOxLineParser();
        String[] lines = entryStr.split("\n");

        List<String> filteredLines =
                Arrays.stream(lines)
                        .filter(line -> line.startsWith("OX  ") || line.startsWith("AC   "))
                        .collect(Collectors.toList());

        String acLine =
                filteredLines.stream()
                        .filter(line -> line.startsWith("AC  "))
                        .collect(Collectors.joining("\n"));
        String accession = acParser.parse(acLine + "\n").primaryAcc;

        String oxLine =
                filteredLines.stream()
                        .filter(line -> line.startsWith("OX  "))
                        .collect(Collectors.joining("\n"));
        int organismId = oxParser.parse(oxLine + "\n").taxonomy_id;
        TaxonomyStatisticsBuilder statisticsBuilder = new TaxonomyStatisticsBuilder();
        if(isNotIsoform(accession)) {
            if (lines[0].contains("Unreviewed;")) {
                statisticsBuilder.unreviewedProteinCount(1L);
            } else {
                statisticsBuilder.reviewedProteinCount(1L);
            }
        }
        return new Tuple2<>(String.valueOf(organismId), statisticsBuilder.build());
    }

    private boolean isNotIsoform(String accession) {
        return !accession.contains("-");
    }
}
