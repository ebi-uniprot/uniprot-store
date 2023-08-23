package org.uniprot.store.spark.indexer.proteome.reader;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ProteomeStatisticsReader {
    public static final String PROTEOMES = "Proteomes";
    private final UniProtKBRDDTupleReader uniProtKBReader;

    public ProteomeStatisticsReader(JobParameter parameter) {
        this.uniProtKBReader = new UniProtKBRDDTupleReader(parameter, false);
    }

    public JavaPairRDD<String, ProteomeStatistics> getProteomeStatisticsRDD() {
        return JavaPairRDD.fromJavaRDD(getProteinInfo()
                        .map(getProteomeStatistics())
                        .flatMap(List::iterator))
                .aggregateByKey(new ProteomeStatisticsBuilder().reviewedProteinCount(0L).unreviewedProteinCount(0L).isoformProteinCount(0L).build(),
                        getAggregationMapper(), getAggregationMapper());
    }

    JavaRDD<String> getProteinInfo() {
        return uniProtKBReader.loadFlatFileToRDD();
    }

    private Function<String, List<Tuple2<String, ProteomeStatistics>>> getProteomeStatistics() {
        return entryStr -> {
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
        };
    }

    private static boolean isCanonicalIsoform(String[] lines) {
        String accession = lines[1].split(" {3}")[1].split(";")[0].strip();
        List<Comment> comments = getComments(lines);
        UniProtKBEntry entry = new UniProtKBEntryBuilder(accession, accession, UniProtKBEntryType.SWISSPROT)
                .commentsSet(comments)
                .build();
        return UniProtEntryConverterUtil.isCanonicalIsoform(entry);
    }

    private static List<Comment> getComments(String[] lines) {
        String ccLines = Arrays.stream(lines)
                .filter(line -> line.startsWith("CC       ") || line.startsWith("CC   -!"))
                .collect(Collectors.joining("\n"));
        List<Comment> comments = List.of();
        if (!ccLines.isEmpty()) {
            CcLineTransformer transformer = new CcLineTransformer();
            comments = transformer.transformNoHeader(ccLines);
        }
        return comments;
    }

    private Function2<ProteomeStatistics, ProteomeStatistics, ProteomeStatistics> getAggregationMapper() {
        return (proteomeStatistics1, proteomeStatistics2) -> new ProteomeStatisticsBuilder().reviewedProteinCount(proteomeStatistics1.getReviewedProteinCount() + proteomeStatistics2.getReviewedProteinCount())
                .unreviewedProteinCount(proteomeStatistics1.getUnreviewedProteinCount() + proteomeStatistics2.getUnreviewedProteinCount())
                .isoformProteinCount(proteomeStatistics1.getIsoformProteinCount() + proteomeStatistics2.getIsoformProteinCount()).build();
    }
}
