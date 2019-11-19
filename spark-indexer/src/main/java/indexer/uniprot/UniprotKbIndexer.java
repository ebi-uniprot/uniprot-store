package indexer.uniprot;

import indexer.go.evidence.GoEvidence;
import indexer.go.evidence.GoEvidenceMapper;
import indexer.go.evidence.GoEvidencesRDDReader;
import indexer.go.relations.GoRelationRDDReader;
import indexer.go.relations.GoTerm;
import indexer.taxonomy.TaxonomyRDDReader;
import indexer.uniprot.mapper.*;
import indexer.uniref.MappedUniRef;
import indexer.uniref.UniRefMapper;
import indexer.uniref.UniRefRDDTupleReader;
import indexer.util.SolrUtils;
import indexer.util.SparkUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.core.cv.pathway.UniPathway;
import org.uniprot.core.cv.pathway.UniPathwayFileReader;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.uniprot.UniProtEntry;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.stream.Collectors;

/**
 * @author lgonzales
 * @since 2019-11-12
 */
@Slf4j
public class UniprotKbIndexer {

    public static void prepareSolrIndex(JavaSparkContext sparkContext, ResourceBundle applicationConfig) {
        SparkConf sparkConf = sparkContext.sc().conf();

        JavaPairRDD<String, UniProtEntry> uniProtEntryRDD = UniprotRDDTupleReader.load(sparkContext, applicationConfig);

        JavaPairRDD<String, Iterable<GoEvidence>> goEvidenceRDD = GoEvidencesRDDReader.load(sparkConf, applicationConfig);
        uniProtEntryRDD = joinGoEvidences(uniProtEntryRDD, goEvidenceRDD);


        JavaPairRDD<String, UniProtDocument> uniProtDocumentRDD = convertToUniProtDocument(uniProtEntryRDD, applicationConfig, sparkContext.hadoopConfiguration());

        JavaPairRDD<String, GoTerm> goRelations = GoRelationRDDReader.load(applicationConfig, sparkContext);
        uniProtDocumentRDD = joinGoRelations(uniProtDocumentRDD, goRelations, applicationConfig, sparkContext);

        JavaPairRDD<String, TaxonomyEntry> taxonomyEntryJavaPairRDD = TaxonomyRDDReader.loadWithLineage(sparkContext, applicationConfig);
        uniProtDocumentRDD = joinTaxonomy(uniProtDocumentRDD, taxonomyEntryJavaPairRDD, applicationConfig, sparkContext);

        JavaPairRDD<String, MappedUniRef> uniref50EntryRDD = UniRefRDDTupleReader.load50(sparkConf, applicationConfig);
        uniProtDocumentRDD = joinUniRef(uniProtDocumentRDD, uniref50EntryRDD);

        JavaPairRDD<String, MappedUniRef> uniref90EntryRDD = UniRefRDDTupleReader.load90(sparkConf, applicationConfig);
        uniProtDocumentRDD = joinUniRef(uniProtDocumentRDD, uniref90EntryRDD);

        JavaPairRDD<String, MappedUniRef> uniref100EntryRDD = UniRefRDDTupleReader.load100(sparkConf, applicationConfig);
        uniProtDocumentRDD = joinUniRef(uniProtDocumentRDD, uniref100EntryRDD);

        String hdfsPath = applicationConfig.getString("uniprot.solr.documents.path");
        SolrUtils.saveSolrInputDocumentRDD(uniProtDocumentRDD, hdfsPath);

        log.info("Completed UniprotKB prepare Solr index");
    }

    private static JavaPairRDD<String, UniProtDocument> convertToUniProtDocument(
            JavaPairRDD<String, UniProtEntry> uniProtEntryRDD,
            ResourceBundle applicationConfig,
            Configuration hadoopConfig) {

        Map<String, String> pathway = loadPathway(applicationConfig, hadoopConfig);
        return (JavaPairRDD<String, UniProtDocument>) uniProtEntryRDD
                .mapValues(new UniProtEntryToSolrDocument(pathway));
    }

    private static Map<String, String> loadPathway(
            ResourceBundle applicationConfig,
            Configuration hadoopConfig) {

        String filePath = applicationConfig.getString("pathway.file.path");
        UniPathwayFileReader uniPathwayFileReader = new UniPathwayFileReader();
        List<String> lines = SparkUtils.readLines(filePath, hadoopConfig);
        List<UniPathway> pathwayList = uniPathwayFileReader.parseLines(lines);
        return pathwayList.stream()
                .collect(Collectors.toMap(UniPathway::getName, UniPathway::getAccession));
    }

    /**
     * --- PLEASE NOTE ---
     * To join Taxonomy we are creating an RDD of JavaPairRDD<taxId,accession> extracted from FlatFile OX and OH lines.
     * For example, the protein P26747 will have 2 tuples in the RDD:
     * Tuple2<10754,P26747> for organism
     * Tuple2<90371,P26747> for virus host
     * <p>
     * The Second step is a join between  JavaPairRDD<taxId,accession> and JavaPairRDD<taxId,TaxonomyEntry> received as method parameter
     * and we group by accession, so the result RDD would be a (JavaPairRDD<accession, Iterable<TaxonomyEntry>>).
     * For example, the protein P26747 would have one tuple in the RDD:
     * Tuple2<P26747, Iterable<TaxonomyEntry(10754), TaxonomyEntry(90371)>>
     * <p>
     * The Third and last step is to join JavaPairRDD<accession, Iterable<TaxonomyEntry>> with JavaPairRDD<accession, UniProtDocument>
     * and at this point we can map TaxonomyEntry information into UniProtDocument.
     *
     * @param uniProtDocumentRDD       RDD of JavaPairRDD<accession,UniProtDocument>
     * @param taxonomyEntryJavaPairRDD RDD of JavaPairRDD<taxId,TaxonomyEntry>
     * @param applicationConfig        config
     * @param sparkContext             context
     * @return RDD of JavaPairRDD<accesion, UniProtDocument> with the mapped taxonomy information
     */
    private static JavaPairRDD<String, UniProtDocument> joinTaxonomy(
            JavaPairRDD<String, UniProtDocument> uniProtDocumentRDD,
            JavaPairRDD<String, TaxonomyEntry> taxonomyEntryJavaPairRDD,
            ResourceBundle applicationConfig, JavaSparkContext sparkContext) {

        // JavaPairRDD<taxId,accession> taxonomyMapRDD --> extracted from flat file OX and OH lines
        JavaPairRDD<String, String> taxonomyMapRDD = (JavaPairRDD<String, String>)
                UniprotRDDTupleReader.loadFlatFileToRDD(sparkContext, applicationConfig)
                        .flatMapToPair(new TaxonomyJoinMapper());

        // JavaPairRDD<accession, Iterable<taxonomy>> joinRDD
        JavaPairRDD<String, Iterable<TaxonomyEntry>> joinedRDD = (JavaPairRDD<String, Iterable<TaxonomyEntry>>)
                JavaPairRDD.fromJavaRDD(taxonomyMapRDD.join(taxonomyEntryJavaPairRDD).values())
                        .groupByKey();

        return (JavaPairRDD<String, UniProtDocument>) uniProtDocumentRDD.leftOuterJoin(joinedRDD)
                .mapValues(new TaxonomyEntryToUniProtDocument());
    }

    private static JavaPairRDD<String, UniProtDocument> joinGoRelations(
            JavaPairRDD<String, UniProtDocument> uniProtDocumentRDD,
            JavaPairRDD<String, GoTerm> goRelationsRDD,
            ResourceBundle applicationConfig, JavaSparkContext sparkContext) {

        // JavaPairRDD<goId,accession> goMapRDD --> extracted from flat file DR lines for GO database
        JavaPairRDD<String, String> goMapRDD = (JavaPairRDD<String, String>)
                UniprotRDDTupleReader.loadFlatFileToRDD(sparkContext, applicationConfig)
                        .flatMapToPair(new GoRelationsJoinMapper());


        // JavaPairRDD<accession, Iterable<GoTerm>> joinRDD
        JavaPairRDD<String, Iterable<GoTerm>> joinedRDD = (JavaPairRDD<String, Iterable<GoTerm>>)
                JavaPairRDD.fromJavaRDD(goMapRDD.join(goRelationsRDD).values())
                        .groupByKey();

        return (JavaPairRDD<String, UniProtDocument>) uniProtDocumentRDD.leftOuterJoin(joinedRDD)
                .mapValues(new GoRelationsToUniProtDocument());
    }

    private static JavaPairRDD<String, UniProtDocument> joinUniRef(
            JavaPairRDD<String, UniProtDocument> uniProtDocumentRDD,
            JavaPairRDD<String, MappedUniRef> unirefJavaPair) {

        return (JavaPairRDD<String, UniProtDocument>) uniProtDocumentRDD
                .leftOuterJoin(unirefJavaPair)
                .mapValues(new UniRefMapper());
    }

    private static JavaPairRDD<String, UniProtEntry> joinGoEvidences(
            JavaPairRDD<String, UniProtEntry> uniProtEntryRDD,
            JavaPairRDD<String, Iterable<GoEvidence>> goEvidenceRDD
    ) {
        return (JavaPairRDD<String, UniProtEntry>) uniProtEntryRDD
                .leftOuterJoin(goEvidenceRDD)
                .mapValues(new GoEvidenceMapper());
    }

}
