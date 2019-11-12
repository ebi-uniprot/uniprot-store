package indexer.uniprot.mapper;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.uniprot.core.flatfile.parser.UniprotLineParser;
import org.uniprot.core.flatfile.parser.impl.DefaultUniprotLineParserFactory;
import org.uniprot.core.flatfile.parser.impl.ac.AcLineObject;
import org.uniprot.core.flatfile.parser.impl.oh.OhLineObject;
import org.uniprot.core.flatfile.parser.impl.ox.OxLineObject;
import org.uniprot.core.util.Utils;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author lgonzales
 * @since 2019-11-12
 */
public class TaxonomyJoinMapper implements PairFlatMapFunction<String, String, String> {
    private static final long serialVersionUID = -1472328217700233918L;


    @Override
    public Iterator<Tuple2<String, String>> call(String entryStr) throws Exception {
        final UniprotLineParser<AcLineObject> acParser = new DefaultUniprotLineParserFactory().createAcLineParser();
        final UniprotLineParser<OxLineObject> oxParser = new DefaultUniprotLineParserFactory().createOxLineParser();
        List<Tuple2<String, String>> organismTuple = new ArrayList<>();

        List<String> taxonomyLines = Arrays.stream(entryStr.split("\n"))
                .filter(line -> line.startsWith("OX  ") || line.startsWith("OH   ") || line.startsWith("AC   "))
                .collect(Collectors.toList());

        String acLine = taxonomyLines.stream()
                .filter(line -> line.startsWith("AC  "))
                .collect(Collectors.joining("\n"));
        String accession = acParser.parse(acLine + "\n").primaryAcc;

        String oxLine = taxonomyLines.stream()
                .filter(line -> line.startsWith("OX  "))
                .collect(Collectors.joining("\n"));
        String taxId = String.valueOf(oxParser.parse(oxLine + "\n").taxonomy_id);
        organismTuple.add(new Tuple2<String, String>(taxId, accession));

        String ohLine = taxonomyLines.stream()
                .filter(line -> line.startsWith("OH  "))
                .collect(Collectors.joining("\n"));
        if (Utils.notEmpty(ohLine)) {
            final UniprotLineParser<OhLineObject> ohParser = new DefaultUniprotLineParserFactory().createOhLineParser();
            OhLineObject ohLineObject = ohParser.parse(ohLine + "\n");
            ohLineObject.hosts.forEach(ohValue -> {
                organismTuple.add(new Tuple2<String, String>(String.valueOf(ohValue.tax_id), accession));
            });
        }
        return (Iterator<Tuple2<String, String>>) organismTuple.iterator();
    }

}
