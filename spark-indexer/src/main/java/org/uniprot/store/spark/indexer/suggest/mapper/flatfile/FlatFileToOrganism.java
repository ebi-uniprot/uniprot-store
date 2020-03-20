package org.uniprot.store.spark.indexer.suggest.mapper.flatfile;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.core.flatfile.parser.UniprotKBLineParser;
import org.uniprot.core.flatfile.parser.impl.DefaultUniprotKBLineParserFactory;
import org.uniprot.core.flatfile.parser.impl.ox.OxLineObject;

import scala.Tuple2;

/**
 * This class map from flatFile entry string to a Tuple of Organism Id found in OX line
 *
 * @author lgonzales
 * @since 2020-01-20
 */
public class FlatFileToOrganism implements PairFunction<String, String, String> {

    private static final long serialVersionUID = -4374937704856351325L;

    @Override
    public Tuple2<String, String> call(String entryStr) throws Exception {
        String oxLine =
                Arrays.stream(entryStr.split("\n"))
                        .filter(line -> line.startsWith("OX  "))
                        .collect(Collectors.joining("\n"));
        final UniprotKBLineParser<OxLineObject> oxParser =
                new DefaultUniprotKBLineParserFactory().createOxLineParser();
        String taxId = String.valueOf(oxParser.parse(oxLine + "\n").taxonomy_id);
        return new Tuple2<>(taxId, taxId);
    }
}
