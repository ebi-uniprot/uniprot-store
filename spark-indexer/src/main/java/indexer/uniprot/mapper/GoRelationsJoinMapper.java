package indexer.uniprot.mapper;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.uniprot.core.flatfile.parser.UniprotLineParser;
import org.uniprot.core.flatfile.parser.impl.DefaultUniprotLineParserFactory;
import org.uniprot.core.flatfile.parser.impl.ac.AcLineObject;
import org.uniprot.core.flatfile.parser.impl.dr.DrLineObject;
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
public class GoRelationsJoinMapper implements PairFlatMapFunction<String, String, String> {
    private static final long serialVersionUID = -2452907832200117358L;

    @Override
    public Iterator<Tuple2<String, String>> call(String entryStr) throws Exception {
        final UniprotLineParser<AcLineObject> acParser = new DefaultUniprotLineParserFactory().createAcLineParser();
        List<Tuple2<String, String>> goTuple = new ArrayList<>();

        List<String> goLines = Arrays.stream(entryStr.split("\n"))
                .filter(line -> line.startsWith("DR   GO;") || line.startsWith("AC   "))
                .collect(Collectors.toList());

        String acLine = goLines.stream()
                .filter(line -> line.startsWith("AC  "))
                .collect(Collectors.joining("\n"));
        String accession = acParser.parse(acLine + "\n").primaryAcc;
        String drLine = goLines.stream()
                .filter(line -> line.startsWith("DR   GO;"))
                .collect(Collectors.joining("\n"));
        if (Utils.notEmpty(drLine)) {
            final UniprotLineParser<DrLineObject> drParser = new DefaultUniprotLineParserFactory().createDrLineParser();
            DrLineObject drLineObject = drParser.parse(drLine + "\n");
            drLineObject.drObjects.forEach(drValue -> {
                String goId = drValue.attributes.get(0).replace(" ", "");
                goTuple.add(new Tuple2<String, String>(goId, accession));
            });
        }

        return (Iterator<Tuple2<String, String>>) goTuple.iterator();
    }
}
