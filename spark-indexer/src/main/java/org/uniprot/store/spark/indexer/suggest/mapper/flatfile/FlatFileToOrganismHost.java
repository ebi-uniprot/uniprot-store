package org.uniprot.store.spark.indexer.suggest.mapper.flatfile;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.uniprot.core.flatfile.parser.UniprotLineParser;
import org.uniprot.core.flatfile.parser.impl.DefaultUniprotLineParserFactory;
import org.uniprot.core.flatfile.parser.impl.oh.OhLineObject;
import org.uniprot.core.util.Utils;

import scala.Tuple2;

/**
 * This class map from flatFile entry string to an Iterator of Tuples of Organism Host found in OH
 * lines
 *
 * @author lgonzales
 * @since 2020-01-20
 */
public class FlatFileToOrganismHost implements PairFlatMapFunction<String, String, String> {

    private static final long serialVersionUID = 7532243656183292758L;

    @Override
    public Iterator<Tuple2<String, String>> call(String entryStr) throws Exception {
        List<Tuple2<String, String>> result = new ArrayList<>();
        String ohLine =
                Arrays.stream(entryStr.split("\n"))
                        .filter(line -> line.startsWith("OH  "))
                        .collect(Collectors.joining("\n"));

        if (Utils.notNullNotEmpty(ohLine)) {
            try {
                final UniprotLineParser<OhLineObject> ohParser =
                        new DefaultUniprotLineParserFactory().createOhLineParser();
                OhLineObject ohLineObject = ohParser.parse(ohLine + "\n");
                ohLineObject
                        .getHosts()
                        .forEach(
                                ohValue -> {
                                    String taxId = String.valueOf(ohValue.getTax_id());
                                    result.add(new Tuple2<String, String>(taxId, taxId));
                                });
            } catch (Exception e) {
                throw new RuntimeException("Unable to parse ohline: " + ohLine, e);
            }
        }
        return result.iterator();
    }
}
