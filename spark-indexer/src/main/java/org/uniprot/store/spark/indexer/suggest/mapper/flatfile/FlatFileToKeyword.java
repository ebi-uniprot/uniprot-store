package org.uniprot.store.spark.indexer.suggest.mapper.flatfile;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.uniprot.core.flatfile.parser.UniprotLineParser;
import org.uniprot.core.flatfile.parser.impl.DefaultUniprotLineParserFactory;
import org.uniprot.core.flatfile.parser.impl.kw.KwLineObject;
import org.uniprot.core.util.Utils;

import scala.Tuple2;

/**
 * This class map from flatFile entry string to an Iterator of Keywords
 * found in KW lines
 * @author lgonzales
 * @since 2020-01-16
 */
public class FlatFileToKeyword implements PairFlatMapFunction<String, String, String> {

    private static final long serialVersionUID = 7532243656183292758L;

    @Override
    public Iterator<Tuple2<String, String>> call(String entryStr) throws Exception {
        List<Tuple2<String, String>> result = new ArrayList<>();
        String keywordLines =
                Arrays.stream(entryStr.split("\n"))
                        .filter(line -> line.startsWith("KW  "))
                        .collect(Collectors.joining("\n"));
        if (Utils.notNullOrEmpty(keywordLines)) {
            final UniprotLineParser<KwLineObject> kwParser =
                    new DefaultUniprotLineParserFactory().createKwLineParser();
            KwLineObject kwLineObject = kwParser.parse(keywordLines + "\n");
            kwLineObject.keywords.forEach(
                    kw -> {
                        result.add(new Tuple2<>(kw.toLowerCase(), kw));
                    });
        }

        return result.iterator();
    }
}
