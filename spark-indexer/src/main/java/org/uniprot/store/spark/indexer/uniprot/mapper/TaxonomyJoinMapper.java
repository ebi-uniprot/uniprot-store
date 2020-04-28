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
import org.uniprot.core.flatfile.parser.impl.oh.OhLineObject;
import org.uniprot.core.flatfile.parser.impl.ox.OxLineObject;
import org.uniprot.core.util.Utils;

import scala.Tuple2;

/**
 * Map from entry in FF String format To Iterator of Tuple2{key=taxId, value=accession} extracted
 * from OX and OH lines
 *
 * @author lgonzales
 * @since 2019-11-12
 */
public class TaxonomyJoinMapper implements PairFlatMapFunction<String, String, String> {
    private static final long serialVersionUID = -1472328217700233918L;

    /**
     * @param entryStr entry in FF String format
     * @return Iterator of Tuple2{key=taxId, value=accession} extracted from OX and OH lines
     */
    @Override
    public Iterator<Tuple2<String, String>> call(String entryStr) throws Exception {
        final UniprotKBLineParser<AcLineObject> acParser =
                new DefaultUniprotKBLineParserFactory().createAcLineParser();
        final UniprotKBLineParser<OxLineObject> oxParser =
                new DefaultUniprotKBLineParserFactory().createOxLineParser();
        List<Tuple2<String, String>> organismTuple = new ArrayList<>();

        List<String> taxonomyLines =
                Arrays.stream(entryStr.split("\n"))
                        .filter(
                                line ->
                                        line.startsWith("OX  ")
                                                || line.startsWith("OH   ")
                                                || line.startsWith("AC   "))
                        .collect(Collectors.toList());

        String acLine =
                taxonomyLines.stream()
                        .filter(line -> line.startsWith("AC  "))
                        .collect(Collectors.joining("\n"));
        String accession = acParser.parse(acLine + "\n").primaryAcc;

        String oxLine =
                taxonomyLines.stream()
                        .filter(line -> line.startsWith("OX  "))
                        .collect(Collectors.joining("\n"));
        String taxId = String.valueOf(oxParser.parse(oxLine + "\n").taxonomy_id);
        organismTuple.add(new Tuple2<String, String>(taxId, accession));

        String ohLine =
                taxonomyLines.stream()
                        .filter(line -> line.startsWith("OH  "))
                        .collect(Collectors.joining("\n"));
        if (Utils.notNullNotEmpty(ohLine)) {
            final UniprotKBLineParser<OhLineObject> ohParser =
                    new DefaultUniprotKBLineParserFactory().createOhLineParser();
            OhLineObject ohLineObject = ohParser.parse(ohLine + "\n");
            ohLineObject
                    .getHosts()
                    .forEach(
                            ohValue ->
                                    organismTuple.add(
                                            new Tuple2<String, String>(
                                                    String.valueOf(ohValue.getTax_id()),
                                                    accession)));
        }
        return (Iterator<Tuple2<String, String>>) organismTuple.iterator();
    }
}
