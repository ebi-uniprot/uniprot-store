package indexer.uniprot;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.core.flatfile.parser.UniprotLineParser;
import org.uniprot.core.flatfile.parser.impl.DefaultUniprotLineParserFactory;
import org.uniprot.core.flatfile.parser.impl.SupportingDataMapImpl;
import org.uniprot.core.flatfile.parser.impl.entry.EntryObject;
import org.uniprot.core.flatfile.parser.impl.entry.EntryObjectConverter;
import org.uniprot.core.uniprot.UniProtEntry;
import scala.Serializable;
import scala.Tuple2;

import java.util.ResourceBundle;

/**
 * @author lgonzales
 * @since 2019-10-16
 */
public class UniprotRDDTupleReader {

    private final static String SPLITTER = "\n//\n";

    public static JavaPairRDD<String, UniProtEntry> read(JavaSparkContext jsc, ResourceBundle applicationConfig) {
        String keywordFile = applicationConfig.getString("keyword.file.path");
        String diseaseFile = applicationConfig.getString("disease.file.path");
        String subcellularLocationFile = applicationConfig.getString("subcell.file.path");

        SupportingDataMapImpl supportingDataMap = new SupportingDataMapImpl(keywordFile, diseaseFile,
                "", subcellularLocationFile); //TODO: Remove go evidence from here

        String filePath = applicationConfig.getString("uniprot.flat.file");
        PairFunction<String, String, UniProtEntry> mapper = new FlatFileMapper(supportingDataMap);
        jsc.hadoopConfiguration().set("textinputformat.record.delimiter", SPLITTER);
        return (JavaPairRDD<String, UniProtEntry>) jsc.textFile(filePath)
                .map(e -> e + SPLITTER)
                .mapToPair(mapper);
    }

    static class FlatFileMapper implements PairFunction<String, String, UniProtEntry>, Serializable {

        private static final long serialVersionUID = -349347145801042417L;
        private final SupportingDataMapImpl supportingDataMap;

        public FlatFileMapper(SupportingDataMapImpl supportingDataMap) {
            this.supportingDataMap = supportingDataMap;
        }

        @Override
        public Tuple2<String, UniProtEntry> call(String entryString) throws Exception {
            UniprotLineParser<EntryObject> entryParser = new DefaultUniprotLineParserFactory().createEntryParser();
            EntryObjectConverter entryObjectConverter = new EntryObjectConverter(supportingDataMap, true);

            EntryObject parsed = entryParser.parse(entryString);
            UniProtEntry uniProtEntry = entryObjectConverter.convert(parsed);
            return new Tuple2<>(uniProtEntry.getPrimaryAccession().getValue(), uniProtEntry);
        }

    }
}
