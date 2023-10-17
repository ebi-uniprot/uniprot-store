package org.uniprot.store.spark.indexer.proteome.mapper;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.uniprot.core.flatfile.parser.impl.cc.CcLineTransformer;
import org.uniprot.core.proteome.ProteomeStatistics;
import org.uniprot.core.proteome.impl.ProteomeStatisticsBuilder;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.core.uniprotkb.UniProtKBEntryType;
import org.uniprot.core.uniprotkb.comment.Comment;
import org.uniprot.core.uniprotkb.impl.UniProtKBEntryBuilder;
import org.uniprot.store.spark.indexer.uniprot.converter.UniProtEntryConverterUtil;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class ProteomeStatisticsMapper
        implements PairFlatMapFunction<String, String, ProteomeStatistics> {
    private static final long serialVersionUID = -4019724767488026549L;
    private static final String DR_PROTEOMES = "DR   Proteomes;";

    @Override
    public Iterator<Tuple2<String, ProteomeStatistics>> call(String entryStr) {
        String[] lines = entryStr.split("\n");
        boolean accessionContainsDash = lines[1].split(";")[0].contains("-");
        boolean unreviewed = lines[0].contains("Unreviewed;");

        int isoformCount = accessionContainsDash && !isCanonicalIsoform(lines) ? 1 : 0;
        int unReviewedCount = !accessionContainsDash && unreviewed ? 1 : 0;
        int reviewedCount = !accessionContainsDash && !unreviewed ? 1 : 0;

        ProteomeStatistics proteomeStatistics =
                new ProteomeStatisticsBuilder()
                        .reviewedProteinCount(reviewedCount)
                        .unreviewedProteinCount(unReviewedCount)
                        .isoformProteinCount(isoformCount)
                        .build();

        return Arrays.stream(lines)
                .filter(line -> line.startsWith(DR_PROTEOMES))
                .map(line -> line.split(";")[1].strip())
                .map(proteomeId -> new Tuple2<>(proteomeId, proteomeStatistics))
                .collect(Collectors.toList())
                .iterator();
    }

    private static boolean isCanonicalIsoform(String[] lines) {
        String accession = lines[1].split(" {3}")[1].split(";")[0].strip();
        List<Comment> comments = getComments(lines);
        UniProtKBEntry entry =
                new UniProtKBEntryBuilder(accession, accession, UniProtKBEntryType.SWISSPROT)
                        .commentsSet(comments)
                        .build();
        return UniProtEntryConverterUtil.isCanonicalIsoform(entry);
    }

    private static List<Comment> getComments(String[] lines) {
        String ccLines =
                Arrays.stream(lines)
                        .filter(line -> line.startsWith("CC       ") || line.startsWith("CC   -!"))
                        .collect(Collectors.joining("\n"));
        List<Comment> comments = List.of();
        if (!ccLines.isEmpty()) {
            CcLineTransformer transformer = new CcLineTransformer();
            comments = transformer.transformNoHeader(ccLines);
        }
        return comments;
    }
}
