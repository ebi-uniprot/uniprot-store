package indexer;

import indexer.go.evidence.GoEvidence;
import indexer.go.evidence.GoEvidencesRDDReader;
import indexer.taxonomy.TaxonomyRDDReader;
import indexer.uniprot.UniprotJoin;
import indexer.uniprot.UniprotRDDTupleReader;
import indexer.uniprot.converter.UniprotDocumentConverter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.core.taxonomy.TaxonomyEntry;
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
/*
    public static void main(String[] args) {
        ResourceBundle applicationConfig = loadApplicationProperty();

        SparkConf sparkConf = new SparkConf().setAppName(applicationConfig.getString("spark.application.name"))
                .setMaster(applicationConfig.getString("spark.master"));//.set("spark.driver.host", "localhost");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaPairRDD<String, TaxonomyEntry> taxonomyEntryJavaPairRDD = TaxonomyRDDReader.readTaxonomyNodeWithLineage(sparkContext, applicationConfig);

        String filePath = applicationConfig.getString("uniprot.flat.file");
        sparkContext.hadoopConfiguration().set("textinputformat.record.delimiter", "\n//\n");
        JavaPairRDD<String, String> taxonomyMapRDD = (JavaPairRDD<String, String>) sparkContext.textFile(filePath)
                .flatMapToPair(entryStr -> {
                    String[] lines = entryStr.split("\n");
                    List<Tuple2<String,String>> organismTuple = new ArrayList<>();
                    String accession= lines[1].substring(2,lines[1].indexOf(";")).trim();

                    Arrays.stream(entryStr.split("\n"))
                            .filter(line -> line.startsWith("OX  ") || line.startsWith("OH   "))
                            .map(line -> {
                                String organismId = line.substring(line.indexOf("NCBI_TaxID=")+11);
                                if(organismId.indexOf(";") > 0 ){
                                    organismId = organismId.substring(0,organismId.indexOf(";"));
                                }
                                if (organismId.indexOf(" ") > 0 ){
                                    organismId = organismId.substring(0,organismId.indexOf(" "));
                                }
                                return new Tuple2<String,String>(organismId, accession);
                            })
                            .forEach(organismTuple::add);

                    return (Iterator<Tuple2<String, String>>) organismTuple.iterator();
                });

        // (JavaPairRDD<String, Iterable<TaxonomyEntry>>)
        JavaPairRDD<String, Iterable<TaxonomyEntry>> joinedRDD = (JavaPairRDD<String, Iterable<TaxonomyEntry>>)
                taxonomyMapRDD.join(taxonomyEntryJavaPairRDD)
                .mapToPair(tuple -> tuple._2)
                .groupByKey();


        System.out.println("JOINED UNIPROT WITH TAXONOMY COUNT: " + joinedRDD.count());
        joinedRDD.take(200).forEach(tuple -> {
            System.out.println("1 ID: " + tuple._1());
            tuple._2.forEach(taxonomyEntry -> {
               System.out.println("2 TAXON ID: "+taxonomyEntry.getTaxonId());
            });
            System.out.println("----------------------------");
        });
        sparkContext.close();
    }
*/

    public static void main(String[] args) throws Exception {
        ResourceBundle applicationConfig = loadApplicationProperty();

        SparkConf sparkConf = new SparkConf().setAppName(applicationConfig.getString("spark.application.name"))
                .setMaster(applicationConfig.getString("spark.master"));//.set("spark.driver.host", "localhost");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaPairRDD<String, UniProtEntry> uniProtEntryRDD = UniprotRDDTupleReader.read(sparkContext, applicationConfig, sparkContext.hadoopConfiguration());

        JavaPairRDD<String, Iterable<GoEvidence>> goEvidenceRDD = GoEvidencesRDDReader.readGoEvidences(sparkConf, applicationConfig);
        uniProtEntryRDD = UniprotJoin.joinGoEvidences(uniProtEntryRDD, goEvidenceRDD);


        JavaPairRDD<String, UniProtDocument> uniProtDocumentRDD = UniprotDocumentConverter.convert(uniProtEntryRDD, applicationConfig, sparkContext.hadoopConfiguration());


        JavaPairRDD<String, TaxonomyEntry> taxonomyEntryJavaPairRDD = TaxonomyRDDReader.readTaxonomyNodeWithLineage(sparkContext, applicationConfig);
        uniProtDocumentRDD = UniprotJoin.joinTaxonomy(uniProtDocumentRDD, taxonomyEntryJavaPairRDD, applicationConfig, sparkContext);


/*        JavaPairRDD<String, MappedUniRef> uniref50EntryRDD = UniRefRDDTupleReader.read50(sparkConf, applicationConfig);
        uniProtDocumentRDD = UniprotJoin.joinUniRef(uniProtDocumentRDD,uniref50EntryRDD);

        JavaPairRDD<String, MappedUniRef> uniref90EntryRDD = UniRefRDDTupleReader.read90(sparkConf, applicationConfig);
        uniProtDocumentRDD = UniprotJoin.joinUniRef(uniProtDocumentRDD,uniref90EntryRDD);

        JavaPairRDD<String, MappedUniRef> uniref100EntryRDD = UniRefRDDTupleReader.read100(sparkConf, applicationConfig);
        uniProtDocumentRDD = UniprotJoin.joinUniRef(uniProtDocumentRDD,uniref100EntryRDD);

        SolrUtils.indexDocuments(uniProtDocumentRDD,"uniprot", applicationConfig);*/


        System.out.println("JOINED UNIPROT COUNT: " + uniProtDocumentRDD.count());
        uniProtDocumentRDD.take(200).forEach(tuple -> {
            System.out.println("ACCESSION: " + tuple._1());

            UniProtDocument document = tuple._2();
            System.out.println("DOCUMENT ACCESSION: " + document.accession);

/*            System.out.println("DOCUMENT 50: " + document.unirefCluster50);
            System.out.println("DOCUMENT 90: " + document.unirefCluster90);
            System.out.println("DOCUMENT 100: " + document.unirefCluster100);
            System.out.println("DOCUMENT GO IDS: " + document.goIds.size());
            System.out.println("DOCUMENT PATHWAY IDS: " + document.pathway.size());
            System.out.println("DOCUMENT KEYWORD IDS: " + document.keywords.size());*/

            System.out.println("DOCUMENT ORGANISM IDS: " + document.organismTaxId);
            System.out.println("DOCUMENT LINEAGE IDS: " + document.taxLineageIds.size());
            System.out.println("DOCUMENT LINEAGE NAMES: " + document.organismTaxon.size());
            System.out.println("DOCUMENT ORGANISM HOST IDS: " + document.organismHostIds.size());
            System.out.println("DOCUMENT ORGANISM HOST NAMES: " + document.organismHostNames.size());
            System.out.println(" ------------------------------------- ");
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
