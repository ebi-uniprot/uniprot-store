package indexer.literature;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * Class Responsible to load PIR mapped file to an JavaPairRDD{key=Uniprot accession,
 * value=PubmedId}
 *
 * @author lgonzales
 * @since 2019-12-21
 */
public class LiteraturePubmedFileMapper implements PairFunction<String, String, String> {
    private static final long serialVersionUID = -8880755540151733726L;

    /**
     * @param entryString PIR mapped file line String
     * @return JavaPairRDD{key=Uniprot accession, value=PubmedId}
     */
    @Override
    public Tuple2<String, String> call(String entryString) throws Exception {
        String[] lineFields = entryString.split("\t");
        String accession = lineFields[0];
        String pubmedId = lineFields[2];
        return new Tuple2<>(accession, pubmedId);
    }
}
