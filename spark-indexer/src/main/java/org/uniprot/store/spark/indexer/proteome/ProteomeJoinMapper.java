package org.uniprot.store.spark.indexer.proteome;

import org.apache.spark.api.java.function.Function;
import org.uniprot.core.flatfile.parser.impl.cc.CcLineTransformer;
import org.uniprot.core.proteome.ProteomeStatistics;
import org.uniprot.core.proteome.impl.ProteomeStatisticsBuilder;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.core.uniprotkb.UniProtKBEntryType;
import org.uniprot.core.uniprotkb.comment.Comment;
import org.uniprot.core.uniprotkb.impl.UniProtKBEntryBuilder;
import org.uniprot.store.spark.indexer.uniprot.converter.UniProtEntryConverterUtil;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ProteomeJoinMapper implements Function<String, List<Tuple2<String, ProteomeStatistics>>> {
    public static final String PROTEOMES = "proteomes";

    @Override
    public List<Tuple2<String, ProteomeStatistics>> call(String entryStr) throws Exception {
        String[] lines = entryStr.split("\n");
        String idLine = lines[0];

        boolean reviewed = idLine.contains("Reviewed;");
        int reviewedCount = reviewed ? 1 : 0;
        boolean accessionContainsDash = lines[1].split(";")[0].contains("-");
        boolean isIsoform = reviewed && accessionContainsDash && !isCanonicalIsoform(entryStr);
        ProteomeStatistics proteomeStatistics = new ProteomeStatisticsBuilder().reviewedProteinCount(reviewedCount).unreviewedProteinCount(1 - reviewedCount)
                .isoformProteinCount(isIsoform ? 1 : 0).build();

        return Arrays.stream(lines)
                .filter(line -> line.startsWith(PROTEOMES))
                .map(line -> line.split(";")[1].strip())
                .map(proteomeId -> new Tuple2<>(proteomeId, proteomeStatistics))
                .collect(Collectors.toList());
    }

    private boolean isCanonicalIsoform(String entryStr) {
        String[] entryLineArray = entryStr.split("\n");
        String ccLines = Arrays.stream(entryLineArray).filter(line -> line.startsWith("CC ") || line.startsWith("CC -!"))
                .collect(Collectors.joining("\n"));
        String accession = entryLineArray[1].split(" {3} ")[1].split(";")[0].strip();
        List<Comment> comments = new ArrayList<>();
        if (!ccLines.isEmpty()) {
            final CcLineTransformer transformer = new CcLineTransformer();
            comments = transformer.transformNoHeader(ccLines);
        }
        UniProtKBEntry entry =
                new UniProtKBEntryBuilder(accession, accession, UniProtKBEntryType.SWISSPROT)
                        .commentsSet(comments)
                        .build();
        return UniProtEntryConverterUtil.isCanonicalIsoform(entry);
    }
}
