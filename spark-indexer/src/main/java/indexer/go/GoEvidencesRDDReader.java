package indexer.go;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.uniprot.core.uniprot.evidence.Evidence;
import org.uniprot.core.uniprot.evidence.impl.EvidenceHelper;
import scala.Tuple2;

import java.util.ResourceBundle;

/**
 * @author lgonzales
 * @since 2019-10-13
 */
@Slf4j
public class GoEvidencesRDDReader {


    public static JavaPairRDD<String, Iterable<GoEvidence>> readGoEvidences(SparkConf sparkConf, ResourceBundle applicationConfig) {
        SparkSession spark = SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate();
        return (JavaPairRDD<String, Iterable<GoEvidence>>) spark.read()
                .textFile(applicationConfig.getString("go.evidence.file.path"))
                .toJavaRDD()
                .mapToPair(new GoEvidencesFileMapper())
                .groupByKey();
    }

    static class GoEvidencesFileMapper implements PairFunction<String, String, GoEvidence> {

        private static final long serialVersionUID = 7265825845507683822L;

        @Override
        public Tuple2<String, GoEvidence> call(String line) throws Exception {
            String[] splitedLine = line.split("\t");
            if (splitedLine.length >= 7) {
                String accession = splitedLine[0];
                String goId = splitedLine[1];
                String evidenceValue = splitedLine[6].replace("PMID", "ECO:0000269|PubMed");
                Evidence evidence = EvidenceHelper.parseEvidenceLine(evidenceValue);
                return new Tuple2<>(accession, new GoEvidence(goId, evidence));
            } else {
                log.warn("unable to parse line: '" + line + "' in go evidence file");
            }
            return null;
        }
    }


    public static void main(String[] args) {
        ResourceBundle applicationConfig = ResourceBundle.getBundle("application");

        SparkConf conf = new SparkConf().setAppName(applicationConfig.getString("spark.application.name"))
                .setMaster(applicationConfig.getString("spark.master"));
        System.out.println("******************* STARTING GO EVIDENCES LOAD *************************");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, Iterable<GoEvidence>> goEvidencesDataset = GoEvidencesRDDReader.readGoEvidences(conf, applicationConfig);

        System.out.println("GO EVIDENCES COUNT" + goEvidencesDataset.count());

        goEvidencesDataset.take(100).forEach(tuple -> {
            System.out.println("ACCESSION ID: " + tuple._1());

            Iterable<GoEvidence> item = tuple._2();
            item.forEach(System.out::println);
        });
        sc.close();

        System.out.println("******************* END GO EVIDENCES LOAD *************************");
    }

}
