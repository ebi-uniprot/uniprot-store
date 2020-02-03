package org.uniprot.store.spark.indexer.suggest.mapper.flatfile;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.uniprot.core.flatfile.parser.UniprotLineParser;
import org.uniprot.core.flatfile.parser.impl.DefaultUniprotLineParserFactory;
import org.uniprot.core.flatfile.parser.impl.de.DeLineConverter;
import org.uniprot.core.flatfile.parser.impl.de.DeLineObject;
import org.uniprot.core.uniprot.description.ProteinDescription;
import org.uniprot.core.util.Utils;
import org.uniprot.store.spark.indexer.uniprot.converter.UniProtEntryProteinDescriptionConverter;

import scala.Tuple2;

/**
 * This class map from flatFile entry string to an Iterator of ECs
 * found in protein description lines
 * @author lgonzales
 * @since 2020-01-17
 */
public class FlatFileToEC implements PairFlatMapFunction<String, String, String> {
    private static final long serialVersionUID = -5412098461659376800L;

    @Override
    public Iterator<Tuple2<String, String>> call(String entryStr) throws Exception {
        List<Tuple2<String, String>> result = new ArrayList<>();
        String descriptionsLines =
                Arrays.stream(entryStr.split("\n"))
                        .filter(line -> line.startsWith("DE  "))
                        .collect(Collectors.joining("\n"));

        if (Utils.notNullOrEmpty(descriptionsLines)) {
            final UniprotLineParser<DeLineObject> deParser =
                    new DefaultUniprotLineParserFactory().createDeLineParser();

            DeLineObject deLineObject = deParser.parse(descriptionsLines + "\n");
            DeLineConverter converter = new DeLineConverter();
            ProteinDescription proteinDescription = converter.convert(deLineObject);

            UniProtEntryProteinDescriptionConverter ecConverter =
                    new UniProtEntryProteinDescriptionConverter();
            List<String> ecNumbers = ecConverter.extractProteinDescriptionEcs(proteinDescription);
            ecNumbers.forEach(ec -> result.add(new Tuple2<>(ec, ec)));
        }

        return result.iterator();
    }
}
