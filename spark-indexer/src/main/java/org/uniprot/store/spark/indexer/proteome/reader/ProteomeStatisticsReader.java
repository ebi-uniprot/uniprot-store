package org.uniprot.store.spark.indexer.proteome.reader;

import org.apache.spark.api.java.JavaPairRDD;
import org.uniprot.core.flatfile.parser.impl.cc.CcLineTransformer;
import org.uniprot.core.proteome.ProteomeStatistics;
import org.uniprot.core.proteome.impl.ProteomeStatisticsBuilder;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.core.uniprotkb.UniProtKBEntryType;
import org.uniprot.core.uniprotkb.comment.Comment;
import org.uniprot.core.uniprotkb.impl.UniProtKBEntryBuilder;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.uniprot.UniProtKBRDDTupleReader;
import org.uniprot.store.spark.indexer.uniprot.converter.UniProtEntryConverterUtil;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ProteomeStatisticsReader {
    public static final String PROTEOMES = "proteomes";
    private final UniProtKBRDDTupleReader uniProtKBReader;

    public ProteomeStatisticsReader(JobParameter parameter) {
        this.uniProtKBReader = new UniProtKBRDDTupleReader(parameter, false);
    }

    public JavaPairRDD<String, ProteomeStatistics> getProteomeStatisticsRDD() {
        return uniProtKBReader.loadFlatFileToRDD()
                .map(this::getProteomeStatistics)
                .flatMap(List::iterator)
                .mapToPair(stringProteomeStatisticsTuple2 -> stringProteomeStatisticsTuple2)
                .aggregateByKey(null, this::aggregate, this::aggregate);
    }

    private List<Tuple2<String, ProteomeStatistics>> getProteomeStatistics(String entryStr) {
        String[] lines = entryStr.split("\n");
        String idLine = lines[0];

        boolean reviewed = idLine.contains("Reviewed;");
        long reviewedCount = reviewed ? 1 : 0;
        boolean accessionContainsDash = lines[1].split(";")[0].contains("-");
        boolean isIsoform = reviewed && accessionContainsDash && !isCanonicalIsoform(lines);
        ProteomeStatistics proteomeStatistics = new ProteomeStatisticsBuilder().reviewedProteinCount(reviewedCount).unreviewedProteinCount(1 - reviewedCount)
                .isoformProteinCount(isIsoform ? 1 : 0).build();

        return Arrays.stream(lines)
                .filter(line -> line.startsWith(PROTEOMES))
                .map(line -> line.split(";")[1].strip())
                .map(proteomeId -> new Tuple2<>(proteomeId, proteomeStatistics))
                .collect(Collectors.toList());
    }

    private boolean isCanonicalIsoform(String[] lines) {
        String ccLines = Arrays.stream(lines)
                .filter(line -> line.startsWith("CC       ") || line.startsWith("CC   -!"))
                .collect(Collectors.joining("\n"));
        List<Comment> comments = new ArrayList<>();
        if (!ccLines.isEmpty()) {
            final CcLineTransformer transformer = new CcLineTransformer();
            comments = transformer.transformNoHeader(ccLines);
        }
        String accession = lines[1].split(" {3}")[1].split(";")[0].strip();
        UniProtKBEntry entry =
                new UniProtKBEntryBuilder(accession, accession, UniProtKBEntryType.SWISSPROT)
                        .commentsSet(comments)
                        .build();
        return !UniProtEntryConverterUtil.isCanonicalIsoform(entry);
    }

    private ProteomeStatistics aggregate(ProteomeStatistics proteomeStatistics1, ProteomeStatistics proteomeStatistics2) {
        return new ProteomeStatisticsBuilder().reviewedProteinCount(proteomeStatistics1.getReviewedProteinCount() + proteomeStatistics2.getReviewedProteinCount())
                .unreviewedProteinCount(proteomeStatistics1.getUnreviewedProteinCount() + proteomeStatistics2.getUnreviewedProteinCount())
                .isoformProteinCount(proteomeStatistics1.getIsoformProteinCount() + proteomeStatistics2.getIsoformProteinCount()).build();
    }
}
