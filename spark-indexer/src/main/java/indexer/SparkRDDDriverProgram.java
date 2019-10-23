package indexer;

import indexer.go.GoEvidence;
import indexer.go.GoEvidenceMapper;
import indexer.go.GoEvidencesRDDReader;
import indexer.uniprot.UniprotDocumentConverter;
import indexer.uniprot.UniprotRDDTupleReader;
import indexer.uniref.MappedUniRef;
import indexer.uniref.UniRefMapper;
import indexer.uniref.UniRefRDDTupleReader;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.core.uniprot.UniProtEntry;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Locale;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

/**
 * @author lgonzales
 * @since 2019-10-16
 */
public class SparkRDDDriverProgram {

    public static void main(String[] args) throws Exception {
        ResourceBundle applicationConfig = loadApplicationProperty();

        SparkConf sparkConf = new SparkConf().setAppName(applicationConfig.getString("spark.application.name"))
                .setMaster(applicationConfig.getString("spark.master"));//.set("spark.driver.host", "localhost");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaPairRDD<String, UniProtEntry> uniProtEntryRDD = UniprotRDDTupleReader.read(sparkContext, applicationConfig);

        JavaPairRDD<String, Iterable<GoEvidence>> goEvidenceRDD = GoEvidencesRDDReader.readGoEvidences(sparkConf, applicationConfig);
        uniProtEntryRDD = (JavaPairRDD<String, UniProtEntry>) uniProtEntryRDD
                .leftOuterJoin(goEvidenceRDD)
                .mapValues(new GoEvidenceMapper());

        JavaPairRDD<String, UniProtDocument> uniProtDocumentRDD = UniprotDocumentConverter.convert(uniProtEntryRDD);

        JavaPairRDD<String, MappedUniRef> uniref50EntryRDD = UniRefRDDTupleReader.read50(sparkConf, applicationConfig);
        uniProtDocumentRDD = (JavaPairRDD<String, UniProtDocument>) uniProtDocumentRDD
                .leftOuterJoin(uniref50EntryRDD)
                .mapValues(new UniRefMapper());

        JavaPairRDD<String, MappedUniRef> uniref90EntryRDD = UniRefRDDTupleReader.read90(sparkConf, applicationConfig);
        uniProtDocumentRDD = (JavaPairRDD<String, UniProtDocument>) uniProtDocumentRDD
                .leftOuterJoin(uniref90EntryRDD)
                .mapValues(new UniRefMapper());

        JavaPairRDD<String, MappedUniRef> uniref100EntryRDD = UniRefRDDTupleReader.read100(sparkConf, applicationConfig);
        uniProtDocumentRDD = (JavaPairRDD<String, UniProtDocument>) uniProtDocumentRDD
                .leftOuterJoin(uniref100EntryRDD)
                .mapValues(new UniRefMapper());

        System.out.println("JOINED UNIPROT COUNT: " + uniProtDocumentRDD.count());
        uniProtDocumentRDD.take(100).forEach(tuple -> {
            System.out.println("ACCESSION: " + tuple._1());

            UniProtDocument document = tuple._2();
            System.out.println("DOCUMENT ACCESSION: " + document.accession);
            System.out.println("DOCUMENT 50: " + document.unirefCluster50);
            System.out.println("DOCUMENT 90: " + document.unirefCluster90);
            System.out.println("DOCUMENT 100: " + document.unirefCluster100);
        });

        sparkContext.close();
    }


    private static ResourceBundle loadApplicationProperty() {
        try {
            //try to load from the directory that the application is being executed
            URL resourceURL = SparkRDDDriverProgram.class.getProtectionDomain().getCodeSource().getLocation();
            URLClassLoader urlLoader = new URLClassLoader(new java.net.URL[]{resourceURL});
            return ResourceBundle.getBundle("application", Locale.getDefault(), urlLoader);
        } catch (MissingResourceException e) {
            // load from the classpath
            return ResourceBundle.getBundle("application");
        }
    }
}
