package org.uniprot.store.spark.indexer.uniprot.mapper;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.uniprot.core.flatfile.parser.UniprotKBLineParser;
import org.uniprot.core.flatfile.parser.impl.DefaultUniprotKBLineParserFactory;
import org.uniprot.core.flatfile.parser.impl.ac.AcLineObject;
import org.uniprot.core.flatfile.parser.impl.cc.cclineobject.*;
import org.uniprot.core.uniprotkb.feature.UniProtKBFeature;
import org.uniprot.core.uniprotkb.feature.UniprotKBFeatureDatabase;
import org.uniprot.core.uniprotkb.feature.UniprotKBFeatureType;
import org.uniprot.core.util.Utils;
import org.uniprot.store.spark.indexer.suggest.SuggesterUtil;

import scala.Tuple2;

public class ChebiJoinMapper implements PairFlatMapFunction<String, String, String> {
    private static final long serialVersionUID = -2452907832200117358L;

    /**
     * @param entryStr flat file entry in String format
     * @return Iterator of Tuple2{key=chebiId, value=accession} extracted from DR lines
     */
    @Override
    public Iterator<Tuple2<String, String>> call(String entryStr) throws Exception {
        Set<Tuple2<String, String>> chebiTuple = new HashSet<>();
        if (entryStr.contains("CC   -!- CATALYTIC ACTIVITY:")
                || entryStr.contains("CC   -!- COFACTOR:")) {
            List<String> extractedLines =
                    Arrays.stream(entryStr.split("\n"))
                            .filter(line -> line.startsWith("CC   ") || line.startsWith("AC   "))
                            .collect(Collectors.toList());

            String accession = getAccesion(extractedLines);

            String commentLine =
                    extractedLines.stream()
                            .filter(
                                    line ->
                                            line.startsWith("CC   ")
                                                    && !line.startsWith("CC   ------")
                                                    && !line.startsWith("CC   Copyrighted")
                                                    && !line.startsWith("CC   Distributed"))
                            .collect(Collectors.joining("\n"));

            final UniprotKBLineParser<CcLineObject> drParser =
                    new DefaultUniprotKBLineParserFactory().createCcLineParser();
            CcLineObject ccLineObject = drParser.parse(commentLine + "\n");
            ccLineObject.getCcs().stream()
                    .filter(
                            cc ->
                                    CC.CCTopicEnum.CATALYTIC_ACTIVITY.equals(cc.getTopic())
                                            || CC.CCTopicEnum.COFACTOR.equals(cc.getTopic()))
                    .forEach(
                            ccValue -> {
                                if (ccValue.getObject() instanceof CatalyticActivity) {
                                    CatalyticActivity catalytic =
                                            (CatalyticActivity) ccValue.getObject();
                                    if (catalytic.getReaction() != null
                                            && catalytic.getReaction().getXref() != null) {
                                        String[] xrefs =
                                                catalytic.getReaction().getXref().split(",");
                                        Arrays.stream(xrefs)
                                                .filter(xref -> xref.contains("CHEBI:"))
                                                .map(
                                                        xref ->
                                                                xref.substring(
                                                                        xref.indexOf("CHEBI:") + 6))
                                                .forEach(
                                                        chebiId ->
                                                                chebiTuple.add(
                                                                        new Tuple2<String, String>(
                                                                                chebiId,
                                                                                accession)));
                                    }
                                }

                                if (ccValue.getObject() instanceof StructuredCofactor) {
                                    StructuredCofactor cofactor =
                                            (StructuredCofactor) ccValue.getObject();
                                    cofactor.getCofactors().stream()
                                            .map(CofactorItem::getXref)
                                            .filter(Objects::nonNull)
                                            .map(xref -> xref.substring(xref.indexOf("CHEBI:") + 6))
                                            .forEach(
                                                    chebiId ->
                                                            chebiTuple.add(
                                                                    new Tuple2<String, String>(
                                                                            chebiId, accession)));
                                    ;
                                }
                            });
        }
        if (entryStr.contains("FT   BINDING         ")) {
            List<String> extractedLines =
                    Arrays.stream(entryStr.split("\n"))
                            .filter(line -> line.startsWith("FT   ") || line.startsWith("AC   "))
                            .collect(Collectors.toList());

            String accession = getAccesion(extractedLines);

            List<UniProtKBFeature> features =
                    SuggesterUtil.getFeaturesByType(
                            String.join("\n", extractedLines), UniprotKBFeatureType.BINDING);
            features.stream()
                    .filter(feature -> Utils.notNullNotEmpty(feature.getFeatureCrossReferences()))
                    .flatMap(feature -> feature.getFeatureCrossReferences().stream())
                    .filter(xref -> xref.getDatabase() == UniprotKBFeatureDatabase.CHEBI)
                    .forEach(
                            xref -> {
                                String id = xref.getId();
                                if (id.startsWith("CHEBI:")) {
                                    id = id.substring("CHEBI:".length());
                                }
                                chebiTuple.add(new Tuple2<>(id, accession));
                            });
        }
        return chebiTuple.iterator();
    }

    private String getAccesion(List<String> extractedLines) {
        final UniprotKBLineParser<AcLineObject> acParser =
                new DefaultUniprotKBLineParserFactory().createAcLineParser();
        String acLine =
                extractedLines.stream()
                        .filter(line -> line.startsWith("AC  "))
                        .collect(Collectors.joining("\n"));
        String accession = acParser.parse(acLine + "\n").primaryAcc;
        return accession;
    }
}
