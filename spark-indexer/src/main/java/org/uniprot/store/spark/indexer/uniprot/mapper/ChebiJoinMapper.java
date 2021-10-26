package org.uniprot.store.spark.indexer.uniprot.mapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.uniprot.core.flatfile.parser.UniprotKBLineParser;
import org.uniprot.core.flatfile.parser.impl.DefaultUniprotKBLineParserFactory;
import org.uniprot.core.flatfile.parser.impl.ac.AcLineObject;
import org.uniprot.core.flatfile.parser.impl.cc.cclineobject.*;

import scala.Tuple2;

public class ChebiJoinMapper implements PairFlatMapFunction<String, String, String> {
    private static final long serialVersionUID = -2452907832200117358L;

    /**
     * @param entryStr flat file entry in String format
     * @return Iterator of Tuple2{key=chebiId, value=accession} extracted from DR lines
     */
    @Override
    public Iterator<Tuple2<String, String>> call(String entryStr) throws Exception {
        List<Tuple2<String, String>> chebiTuple = new ArrayList<>();
        if (entryStr.contains("CC   -!- CATALYTIC ACTIVITY:")
                || entryStr.contains("CC   -!- COFACTOR:")) {
            final UniprotKBLineParser<AcLineObject> acParser =
                    new DefaultUniprotKBLineParserFactory().createAcLineParser();
            List<String> extractedLines =
                    Arrays.stream(entryStr.split("\n"))
                            .filter(line -> line.startsWith("CC   ") || line.startsWith("AC   "))
                            .collect(Collectors.toList());

            String acLine =
                    extractedLines.stream()
                            .filter(line -> line.startsWith("AC  "))
                            .collect(Collectors.joining("\n"));
            String accession = acParser.parse(acLine + "\n").primaryAcc;

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
                                            .filter(xref -> xref.getXref() != null)
                                            .map(CofactorItem::getXref)
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
        return chebiTuple.iterator();
    }
}
