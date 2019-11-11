package indexer;

import indexer.go.evidence.GoEvidence;
import indexer.go.evidence.GoEvidencesRDDReader;
import indexer.go.relations.GoRelationRDDReader;
import indexer.go.relations.GoTerm;
import indexer.taxonomy.TaxonomyRDDReader;
import indexer.uniprot.UniprotJoin;
import indexer.uniprot.UniprotRDDTupleReader;
import indexer.uniprot.converter.UniprotDocumentConverter;
import indexer.uniref.MappedUniRef;
import indexer.uniref.UniRefRDDTupleReader;
import indexer.util.SolrUtils;
import indexer.util.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.uniprot.UniProtEntry;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

import java.util.ResourceBundle;

/**
 * @author lgonzales
 * @since 2019-10-16
 */
public class SparkRDDDriverProgram {

    public static void main(String[] args) throws Exception {
        ResourceBundle applicationConfig = SparkUtils.loadApplicationProperty();

        SparkConf sparkConf = new SparkConf().setAppName(applicationConfig.getString("spark.application.name"))
                .setMaster(applicationConfig.getString("spark.master"));//.set("spark.driver.host", "localhost");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaPairRDD<String, UniProtEntry> uniProtEntryRDD = UniprotRDDTupleReader.read(sparkContext, applicationConfig, sparkContext.hadoopConfiguration());

        JavaPairRDD<String, Iterable<GoEvidence>> goEvidenceRDD = GoEvidencesRDDReader.readGoEvidences(sparkConf, applicationConfig);
        uniProtEntryRDD = UniprotJoin.joinGoEvidences(uniProtEntryRDD, goEvidenceRDD);


        JavaPairRDD<String, UniProtDocument> uniProtDocumentRDD = UniprotDocumentConverter.convert(uniProtEntryRDD, applicationConfig, sparkContext.hadoopConfiguration());

        JavaPairRDD<String, GoTerm> goRelations = GoRelationRDDReader.loadGoRelations(applicationConfig, sparkContext);
        uniProtDocumentRDD = UniprotJoin.joinGoRelations(uniProtDocumentRDD, goRelations, applicationConfig, sparkContext);

        JavaPairRDD<String, TaxonomyEntry> taxonomyEntryJavaPairRDD = TaxonomyRDDReader.readTaxonomyNodeWithLineage(sparkContext, applicationConfig);
        uniProtDocumentRDD = UniprotJoin.joinTaxonomy(uniProtDocumentRDD, taxonomyEntryJavaPairRDD, applicationConfig, sparkContext);

        JavaPairRDD<String, MappedUniRef> uniref50EntryRDD = UniRefRDDTupleReader.read50(sparkConf, applicationConfig);
        uniProtDocumentRDD = UniprotJoin.joinUniRef(uniProtDocumentRDD,uniref50EntryRDD);

        JavaPairRDD<String, MappedUniRef> uniref90EntryRDD = UniRefRDDTupleReader.read90(sparkConf, applicationConfig);
        uniProtDocumentRDD = UniprotJoin.joinUniRef(uniProtDocumentRDD,uniref90EntryRDD);

        JavaPairRDD<String, MappedUniRef> uniref100EntryRDD = UniRefRDDTupleReader.read100(sparkConf, applicationConfig);
        uniProtDocumentRDD = UniprotJoin.joinUniRef(uniProtDocumentRDD,uniref100EntryRDD);

        String hdfsPath = applicationConfig.getString("uniprot.solr.documents.path");
        SolrUtils.saveSolrInputDocumentRDD(uniProtDocumentRDD, hdfsPath);


        //System.out.println("JOINED UNIPROT COUNT: " + uniProtDocumentRDD.count());
/*        uniProtDocumentRDD.take(200).forEach(tuple -> {
            System.out.println("ACCESSION: " + tuple._1());

            UniProtDocument document = tuple._2();
            System.out.println("DOCUMENT ACCESSION: " + document.accession);
            System.out.println("DOCUMENT 50: " + document.unirefCluster50);
            System.out.println("DOCUMENT 90: " + document.unirefCluster90);
            System.out.println("DOCUMENT 100: " + document.unirefCluster100);
            System.out.println("DOCUMENT GO IDS: " + document.goIds.size());
            System.out.println("DOCUMENT PATHWAY IDS: " + document.pathway.size());
            System.out.println("DOCUMENT KEYWORD IDS: " + document.keywords.size());
            System.out.println("DOCUMENT ORGANISM IDS: " + document.organismTaxId);
            System.out.println("DOCUMENT LINEAGE IDS: " + document.taxLineageIds.size());
            System.out.println("DOCUMENT LINEAGE NAMES: " + document.organismTaxon.size());
            System.out.println("DOCUMENT ORGANISM HOST IDS: " + document.organismHostIds.size());
            System.out.println("DOCUMENT ORGANISM HOST NAMES: " + document.organismHostNames.size());
            System.out.println("-------------------------------------");
        });*/


        sparkContext.close();
    }


}
