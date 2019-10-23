package indexer.disease;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.core.cv.disease.Disease;
import org.uniprot.core.cv.disease.DiseaseFileReader;
import org.uniprot.core.util.Utils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.ResourceBundle;

/**
 * @author lgonzales
 * @since 2019-10-13
 */
public class DiseaseRDDReader {

    private final static String SPLITTER = "\n//\n";

    public static JavaPairRDD<String, Disease> readDisease(JavaSparkContext jsc, ResourceBundle applicationConfig) {
        String filePath = applicationConfig.getString("disease.file.path");
        jsc.hadoopConfiguration().set("textinputformat.record.delimiter", SPLITTER);

        return (JavaPairRDD<String, Disease>) jsc.textFile(filePath)
                .map(e -> "______\n" + e + SPLITTER)
                .mapToPair(new DiseaseFileMapper());
    }

    static class DiseaseFileMapper implements PairFunction<String, String, Disease> {

        @Override
        public Tuple2<String, Disease> call(String diseaseLines) throws Exception {
            DiseaseFileReader fileReader = new DiseaseFileReader();
            List<String> diseaseLineList = Arrays.asList(diseaseLines.split("\n"));
            List<Disease> diseases = fileReader.parseLines(diseaseLineList);
            if (Utils.notEmpty(diseases)) {
                Disease disease = diseases.get(0);
                return new Tuple2(disease.getId(), disease);
            } else {
                System.out.println("ERROR WITH: " + diseaseLines);
                return null;
            }
        }
    }

    public static void main(String[] args) {
        ResourceBundle applicationConfig = ResourceBundle.getBundle("application");

        SparkConf conf = new SparkConf().setAppName(applicationConfig.getString("spark.application.name"))
                .setMaster(applicationConfig.getString("spark.master"));
        System.out.println("******************* STARTING DISEASE LOAD *************************");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, Disease> diseaseDataset = DiseaseRDDReader.readDisease(sc, applicationConfig);

        System.out.println("DISEASE COUNT" + diseaseDataset.count());

        diseaseDataset.take(100).forEach(tuple -> {
            System.out.println("DISEASE ID: " + tuple._1());

            Disease item = tuple._2();
            System.out.println("DISEASE ENTRY ACCESSION: " + item.getAccession());
        });
        sc.close();

        System.out.println("******************* END DISEASE LOAD *************************");
    }
}
