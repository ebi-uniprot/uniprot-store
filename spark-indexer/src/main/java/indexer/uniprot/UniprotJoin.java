package indexer.uniprot;

import indexer.go.evidence.GoEvidence;
import indexer.go.evidence.GoEvidenceMapper;
import indexer.go.relations.GoRelationRDDReader;
import indexer.go.relations.GoTerm;
import indexer.uniprot.converter.UniprotDocumentConverter;
import indexer.uniref.MappedUniRef;
import indexer.uniref.UniRefMapper;
import indexer.util.SparkUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.uniprot.core.flatfile.parser.UniprotLineParser;
import org.uniprot.core.flatfile.parser.impl.DefaultUniprotLineParserFactory;
import org.uniprot.core.flatfile.parser.impl.ac.AcLineObject;
import org.uniprot.core.flatfile.parser.impl.dr.DrLineObject;
import org.uniprot.core.flatfile.parser.impl.oh.OhLineObject;
import org.uniprot.core.flatfile.parser.impl.ox.OxLineObject;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.uniprot.UniProtEntry;
import org.uniprot.core.util.Utils;
import org.uniprot.store.search.document.uniprot.UniProtDocument;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author lgonzales
 * @since 2019-10-31
 */
@Slf4j
public class UniprotJoin {


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
    public static JavaPairRDD<String, UniProtDocument> joinTaxonomy(
            JavaPairRDD<String, UniProtDocument> uniProtDocumentRDD,
            JavaPairRDD<String, TaxonomyEntry> taxonomyEntryJavaPairRDD,
            ResourceBundle applicationConfig, JavaSparkContext sparkContext) {

        // JavaPairRDD<taxId,accession> taxonomyMapRDD --> extracted from flat file OX and OH lines
        JavaPairRDD<String, String> taxonomyMapRDD = (JavaPairRDD<String, String>) extractDataFromFlatFile(applicationConfig, sparkContext, new TaxonomyIdAndUniprotAccessionMapper());

        // JavaPairRDD<accession, Iterable<taxonomy>> joinRDD
        JavaPairRDD<String, Iterable<TaxonomyEntry>> joinedRDD = (JavaPairRDD<String, Iterable<TaxonomyEntry>>)
                JavaPairRDD.fromJavaRDD(taxonomyMapRDD.join(taxonomyEntryJavaPairRDD).values())
                        .groupByKey().repartition(uniProtDocumentRDD.getNumPartitions());

        return (JavaPairRDD<String, UniProtDocument>) uniProtDocumentRDD.leftOuterJoin(joinedRDD)
                .mapValues(new TaxonomyIdUniProtDocumentMapper());
    }


    public static JavaPairRDD<String, UniProtDocument> joinGoRelations(
            JavaPairRDD<String, UniProtDocument> uniProtDocumentRDD,
            JavaPairRDD<String, GoTerm> goRelationsRDD,
            ResourceBundle applicationConfig, JavaSparkContext sparkContext) {

        // JavaPairRDD<goId,accession> goMapRDD --> extracted from flat file DR lines for GO database
        JavaPairRDD<String, String> goMapRDD = (JavaPairRDD<String, String>) extractDataFromFlatFile(applicationConfig, sparkContext, new GOIdAndUniprotAccessionMapper());


        // JavaPairRDD<accession, Iterable<GoTerm>> joinRDD
        JavaPairRDD<String, Iterable<GoTerm>> joinedRDD = (JavaPairRDD<String, Iterable<GoTerm>>)
                JavaPairRDD.fromJavaRDD(goMapRDD.join(goRelationsRDD).values())
                        .groupByKey().repartition(uniProtDocumentRDD.getNumPartitions());

        return (JavaPairRDD<String, UniProtDocument>) uniProtDocumentRDD.leftOuterJoin(joinedRDD)
                .mapValues(new GoRelationsUniProtDocumentMapper());
    }

    public static JavaPairRDD<String, UniProtDocument> joinUniRef(
            JavaPairRDD<String, UniProtDocument> uniProtDocumentRDD,
            JavaPairRDD<String, MappedUniRef> unirefJavaPair) {

        return (JavaPairRDD<String, UniProtDocument>) uniProtDocumentRDD
                .leftOuterJoin(unirefJavaPair)
                .mapValues(new UniRefMapper());
    }

    public static JavaPairRDD<String, UniProtEntry> joinGoEvidences(
            JavaPairRDD<String, UniProtEntry> uniProtEntryRDD,
            JavaPairRDD<String, Iterable<GoEvidence>> goEvidenceRDD
    ) {
        return (JavaPairRDD<String, UniProtEntry>) uniProtEntryRDD
                .leftOuterJoin(goEvidenceRDD)
                .mapValues(new GoEvidenceMapper());
    }

    private static JavaPairRDD<String, String> extractDataFromFlatFile(ResourceBundle applicationConfig,
                                                                       JavaSparkContext sparkContext,
                                                                       PairFlatMapFunction<String, String, String> mapper) {
        String filePath = applicationConfig.getString("uniprot.flat.file");
        sparkContext.hadoopConfiguration().set("textinputformat.record.delimiter", "\n//\n");
        return (JavaPairRDD<String, String>) sparkContext.textFile(filePath).flatMapToPair(mapper);
    }


    private static class TaxonomyIdAndUniprotAccessionMapper implements PairFlatMapFunction<String, String, String> {

        private static final long serialVersionUID = -1333694971232779502L;

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

    private static class TaxonomyIdUniProtDocumentMapper implements Function<Tuple2<UniProtDocument, Optional<Iterable<TaxonomyEntry>>>, UniProtDocument> {

        private static final long serialVersionUID = -2472089731133867911L;

        @Override
        public UniProtDocument call(Tuple2<UniProtDocument, Optional<Iterable<TaxonomyEntry>>> tuple) throws Exception {
            UniProtDocument doc = tuple._1;
            if (tuple._2.isPresent()) {
                Map<Long, TaxonomyEntry> taxonomyEntryMap = new HashMap<>();
                tuple._2.get().forEach(taxonomyEntry -> {
                    taxonomyEntryMap.put(taxonomyEntry.getTaxonId(), taxonomyEntry);
                });

                TaxonomyEntry organism = taxonomyEntryMap.get((long) doc.organismTaxId);
                if (organism != null) {
                    updateOrganismFields(doc, organism);
                } else {
                    log.warn("Unable to find organism id " + doc.organismTaxId + " in mapped organisms " + taxonomyEntryMap.keySet());
                }

                if (Utils.notEmpty(doc.organismHostIds)) {
                    doc.organismHostIds.forEach(taxId -> {
                        TaxonomyEntry organismHost = taxonomyEntryMap.get((long) taxId);
                        if (organismHost != null) {
                            doc.organismHostNames.addAll(getOrganismNames(organismHost));
                        } else {
                            log.warn("Unable to find organism host id " + taxId + " in mapped organisms " + taxonomyEntryMap.keySet());
                        }
                    });
                    doc.content.addAll(doc.organismHostNames);
                }
            } else {
                log.warn("Unable to join organism for " + doc.organismTaxId + " in document: " + doc.accession);
            }
            return doc;
        }

        private void updateOrganismFields(UniProtDocument doc, TaxonomyEntry organism) {
            doc.organismName.addAll(getOrganismNames(organism));
            if (organism.hasLineage()) {
                organism.getLineage()
                        .forEach(lineage -> {
                            doc.taxLineageIds.add(new Long(lineage.getTaxonId()).intValue());
                            doc.organismTaxon.add(lineage.getScientificName());
                            if (lineage.hasCommonName()) {
                                doc.organismTaxon.add(lineage.getCommonName());
                            }
                        });
            } else {
                log.warn("Unable to find organism lineage for: " + doc.organismTaxId);
            }
            doc.content.addAll(doc.organismName);
            doc.content.addAll(doc.taxLineageIds.stream().map(String::valueOf).collect(Collectors.toList()));
            doc.content.addAll(doc.organismTaxon);
        }

        private List<String> getOrganismNames(TaxonomyEntry organism) {
            List<String> organismNames = new ArrayList<>();
            organismNames.add(organism.getScientificName());
            if (organism.hasCommonName()) {
                organismNames.add(organism.getCommonName());
            }
            if (organism.hasSynonyms()) {
                organismNames.addAll(organism.getSynonyms());
            }
            if (organism.hasMnemonic()) {
                organismNames.add(organism.getMnemonic());
            }
            return organismNames;
        }
    }


    private static class GOIdAndUniprotAccessionMapper implements PairFlatMapFunction<String, String, String> {


        private static final long serialVersionUID = -1766382369766962571L;

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

    @Slf4j
    private static class GoRelationsUniProtDocumentMapper implements Function<Tuple2<UniProtDocument, Optional<Iterable<GoTerm>>>, UniProtDocument> {

        private static final long serialVersionUID = 158933287443925883L;

        @Override
        public UniProtDocument call(Tuple2<UniProtDocument, Optional<Iterable<GoTerm>>> tuple) throws Exception {
            UniProtDocument doc = tuple._1;
            if (tuple._2.isPresent()) {
                tuple._2.get().forEach(goTerm -> {
                    String parentId = goTerm.getId().substring(3).trim();
                    String evidenceMapKey = getGoWithEvidenceMapsKey(parentId, doc.goWithEvidenceMaps);
                    Collection<String> goMapValues = doc.goWithEvidenceMaps.get(evidenceMapKey);
                    if (goTerm.getAncestors() != null) {
                        goTerm.getAncestors().forEach(ancestor -> {
                            String idOnly = ancestor.getId().substring(3).trim();
                            doc.goIds.add(idOnly);
                            doc.goes.add(idOnly);
                            doc.goes.add(ancestor.getName());

                            if (goMapValues != null) {
                                goMapValues.add(idOnly);
                                goMapValues.add(ancestor.getName());
                            } else {
                                log.warn("Unable to find Go With Evidence Type Map Key");
                            }
                        });
                    }
                });
            }
            return doc;
        }

        private String getGoWithEvidenceMapsKey(String idOnly, Map<String, Collection<String>> goWithEvidenceMaps) {
            String result = "";
            for (Map.Entry<String, Collection<String>> mapEntry : goWithEvidenceMaps.entrySet()) {
                if (mapEntry.getValue().contains(idOnly)) {
                    result = mapEntry.getKey();
                    break;
                }
            }
            return result;
        }

    }

    public static void main(String[] args) throws Exception {
/*        String goLines = "AC   P21802-14;\n" +
                "DR   Bgee; ENSG00000066468; Expressed in 222 organ(s), highest expression level in C1 segment of cervical spinal cord.\n" +
                "DR   ExpressionAtlas; P21802; baseline and differential.\n" +
                "DR   Genevisible; P21802; HS.\n" +
                "DR   GO; GO:0005938; C:cell cortex; IDA:UniProtKB.\n" +
                "DR   GO; GO:0009986; C:cell surface; IDA:UniProtKB.\n" +
                "DR   GO; GO:0005737; C:cytoplasm; IDA:UniProtKB.\n";

        GOIdAndUniprotAccessionMapper mapper = new GOIdAndUniprotAccessionMapper();
        mapper.call(goLines).forEachRemaining(tuple -> {
            System.out.println(tuple._1+ " "+tuple._2);
        }); */

        ResourceBundle applicationConfig = SparkUtils.loadApplicationProperty();
        SparkConf sparkConf = new SparkConf().setAppName(applicationConfig.getString("spark.application.name"))
                .setMaster(applicationConfig.getString("spark.master"));//.set("spark.driver.host", "localhost");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaPairRDD<String, UniProtEntry> uniProtEntryRDD = UniprotRDDTupleReader.read(sparkContext, applicationConfig, sparkContext.hadoopConfiguration());

        JavaPairRDD<String, UniProtDocument> uniProtDocumentRDD = UniprotDocumentConverter.convert(uniProtEntryRDD, applicationConfig, sparkContext.hadoopConfiguration());

        JavaPairRDD<String, GoTerm> goRelations = GoRelationRDDReader.loadGoRelations(applicationConfig, sparkContext);
        uniProtDocumentRDD = UniprotJoin.joinGoRelations(uniProtDocumentRDD, goRelations, applicationConfig, sparkContext);

        uniProtDocumentRDD.foreach(tuple -> {
            UniProtDocument doc = tuple._2;
            if (doc.accession.equals("P21802-3")) {
                System.out.println(doc.accession);
            }
        });

    }

}
