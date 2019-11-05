package indexer.uniprot;

import indexer.go.evidence.GoEvidence;
import indexer.go.evidence.GoEvidenceMapper;
import indexer.uniref.MappedUniRef;
import indexer.uniref.UniRefMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.uniprot.UniProtEntry;
import org.uniprot.core.util.Utils;
import org.uniprot.store.search.document.uniprot.UniProtDocument;
import scala.Tuple2;

import java.util.*;

/**
 * @author lgonzales
 * @since 2019-10-31
 */
@Slf4j
public class UniprotJoin {


    public static JavaPairRDD<String, UniProtDocument> joinTaxonomy(
            JavaPairRDD<String, UniProtDocument> uniProtDocumentRDD,
            JavaPairRDD<String, TaxonomyEntry> taxonomyEntryJavaPairRDD,
            ResourceBundle applicationConfig, JavaSparkContext sparkContext) {

        // JavaPairRDD<taxId,accession>
        String filePath = applicationConfig.getString("uniprot.flat.file");
        sparkContext.hadoopConfiguration().set("textinputformat.record.delimiter", "\n//\n");
        JavaPairRDD<String, String> taxonomyMapRDD = (JavaPairRDD<String, String>) sparkContext.textFile(filePath)
                .flatMapToPair(entryStr -> {
                    String[] lines = entryStr.split("\n");
                    List<Tuple2<String, String>> organismTuple = new ArrayList<>();
                    String accession = lines[1].substring(2, lines[1].indexOf(";")).trim();

                    Arrays.stream(entryStr.split("\n"))
                            .filter(line -> line.startsWith("OX  ") || line.startsWith("OH   "))
                            .map(line -> {
                                String organismId = line.substring(line.indexOf("NCBI_TaxID=") + 11);
                                if (organismId.indexOf(";") > 0) {
                                    organismId = organismId.substring(0, organismId.indexOf(";"));
                                }
                                if (organismId.indexOf(" ") > 0) {
                                    organismId = organismId.substring(0, organismId.indexOf(" "));
                                }
                                return new Tuple2<String, String>(organismId, accession);
                            })
                            .forEach(organismTuple::add);

                    return (Iterator<Tuple2<String, String>>) organismTuple.iterator();
                });

        // JavaPairRDD<accession, Iterable<taxonomy>>
        JavaPairRDD<String, Iterable<TaxonomyEntry>> joinedRDD = (JavaPairRDD<String, Iterable<TaxonomyEntry>>)
                taxonomyMapRDD.join(taxonomyEntryJavaPairRDD)
                        .mapToPair(tuple -> tuple._2)
                        .groupByKey()
                        .repartition(500);

        return (JavaPairRDD<String, UniProtDocument>) uniProtDocumentRDD.leftOuterJoin(joinedRDD)
                .mapValues(tuple -> {
                    UniProtDocument doc = tuple._1;
                    if (tuple._2.isPresent()) {
                        Map<Long, TaxonomyEntry> taxonomyEntryMap = new HashMap<>();
                        tuple._2.get().forEach(taxonomyEntry -> {
                            taxonomyEntryMap.put(taxonomyEntry.getTaxonId(), taxonomyEntry);
                        });

                        TaxonomyEntry organism = taxonomyEntryMap.get((long) doc.organismTaxId);
                        if (organism != null) {
                            doc.organismName.add(organism.getScientificName());
                            if (organism.hasCommonName()) {
                                doc.organismName.add(organism.getCommonName());
                            }
                            if (organism.hasSynonyms()) {
                                doc.organismName.addAll(organism.getSynonyms());
                            }
                            if (organism.hasMnemonic()) {
                                doc.organismName.add(organism.getMnemonic());
                            }
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
                        } else {
                            log.warn("Unable to find organism id " + doc.organismTaxId + " in mapped organisms " + taxonomyEntryMap.keySet());
                        }

                        if (Utils.notEmpty(doc.organismHostIds)) {
                            doc.organismHostIds.forEach(taxId -> {
                                TaxonomyEntry organismHost = taxonomyEntryMap.get((long) taxId);
                                if (organismHost != null) {
                                    doc.organismHostNames.add(organismHost.getScientificName());
                                    if (organismHost.hasCommonName()) {
                                        doc.organismHostNames.add(organismHost.getCommonName());
                                    }
                                    if (organismHost.hasSynonyms()) {
                                        doc.organismHostNames.addAll(organismHost.getSynonyms());
                                    }
                                    if (organismHost.hasMnemonic()) {
                                        doc.organismHostNames.add(organismHost.getMnemonic());
                                    }
                                } else {
                                    log.warn("Unable to find organism host id " + taxId + " in mapped organisms " + taxonomyEntryMap.keySet());
                                }
                            });
                        }
                    } else {
                        log.warn("Unable to join organism for " + doc.organismTaxId + " in document: " + doc.accession);
                    }
                    return doc;
                });
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


}
