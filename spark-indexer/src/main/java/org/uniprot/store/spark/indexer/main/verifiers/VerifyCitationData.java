package org.uniprot.store.spark.indexer.main.verifiers;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getInputReleaseDirPath;

import java.util.ResourceBundle;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.uniprot.core.citation.Citation;
import org.uniprot.core.citation.CitationDatabase;
import org.uniprot.core.uniprotkb.UniProtKBReference;
import org.uniprot.core.util.Pair;
import org.uniprot.core.util.PairImpl;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;
import org.uniprot.store.spark.indexer.literature.mapper.LiteratureFileMapper;

/**
 * @author lgonzales
 * @since 22/03/2021
 */
@Slf4j
public class VerifyCitationData {

    private static final String SPLITTER = "\n//";

    public static void main(String[] args) {
        ResourceBundle config = SparkUtils.loadApplicationProperty();
        JavaSparkContext sparkContext = SparkUtils.loadSparkContext(config);

        JobParameter jobParameter =
                JobParameter.builder()
                        .applicationConfig(config)
                        .releaseName("2021_01")
                        .sparkContext(sparkContext)
                        .build();

        for (String file : args) {
            log.info("---------------- Literature File Name: {} ---------------------", file);
            String releaseInputDir = getInputReleaseDirPath(config, jobParameter.getReleaseName());
            String literaturePath = releaseInputDir + "literature/" + file;
            sparkContext.hadoopConfiguration().set("textinputformat.record.delimiter", SPLITTER);
            long count =
                    sparkContext
                            .textFile(literaturePath, 1000)
                            .mapToPair(new LiteratureFileMapper())
                            .count();
            log.info("Number of entries: {}", count);
            /*            List<Tuple2<String, Literature>> literature = sparkContext.textFile(literaturePath, 1000)
                    .mapToPair(new LiteratureFileMapper())
                    .take(10);
            literature.forEach(lit ->  {
                log.info("Key : {}",lit._1);
                log.info("Value : {}",lit._2.getId());
            });
            log.info("---------------- THE END FILE ------------------");*/
        }

        /*        JavaRDD<String> solrInputDocumentRDD =
                new UniProtKBRDDTupleReader(jobParameter, false).loadFlatFileToRDD();

        long result =
                solrInputDocumentRDD
                        .flatMapToPair(new LiteratureEntryUniProtKBMapper())
                        .aggregateByKey(
                                null,
                                new LiteratureEntryAggregationMapper(),
                                new LiteratureEntryAggregationMapper())
                        .count();
        log.info("Total Ids: {}", result); // Total Ids: 828.638

        log.info("The End");

                long withDoi = solrInputDocumentRDD
                        .flatMap(new UniProtKBPublicationMapper())
                        .filter(VerifyCitationData::hasDoi)
                        .count();
                log.info("withDoi: {} ", withDoi);

                long withAgricola = solrInputDocumentRDD
                        .flatMap(new UniProtKBPublicationMapper())
                        .filter(VerifyCitationData::hasAgricola)
                        .count();

                log.info("withAgricola: {} ", withAgricola);

        log.info("------------------------------------------------------------------------------");
        long hashIds =
                solrInputDocumentRDD
                        .flatMap(new UniProtKBPublicationMapper())
                        .filter(VerifyCitationData::noIds)
                        .map(UniProtKBReference::getCitation)
                        .map(Citation::getId)
                        .distinct()
                        .count();
        log.info("distinct hashIds: {}", hashIds);

        log.info("-------------------------------------------------------------------------------");
        Map<String, Long> idsTypeCount =
                solrInputDocumentRDD
                        .flatMap(new UniProtKBPublicationMapper())
                        .filter(VerifyCitationData::hasId)
                        .map(VerifyCitationData::mapByType)
                        .countByValue();
        idsTypeCount.forEach(
                (key, value) -> log.info("idsTypeCount: Type: {} , count: {} ", key, value));
        log.info(
                "--------------------------------------------------------------------------------");*/
        sparkContext.close();
    }

    private static class AreEquals
            implements Function<Iterable<Citation>, Pair<Citation, Citation>> {

        private static final long serialVersionUID = 4600560402871146686L;

        @Override
        public Pair<Citation, Citation> call(Iterable<Citation> entries) throws Exception {
            Citation first = null;
            Citation notEquals = null;
            for (Citation entry : entries) {
                if (first == null) {
                    first = entry;
                } else if (!first.equals(entry)) {
                    notEquals = entry;
                    break;
                }
            }
            return new PairImpl<>(first, notEquals);
        }
    }

    private static boolean hasId(UniProtKBReference reference) {
        boolean result = false;
        if (reference.hasCitation() && reference.getCitation().hasCitationCrossReferences()) {
            result = true;
        }
        return result;
    }

    private static String mapByType(UniProtKBReference reference) {
        String result = "NO-CITATION";
        if (reference.hasCitation()) {
            result = reference.getCitation().getCitationType().name();
        }
        return result;
    }

    private static boolean hasPubmedId(UniProtKBReference reference) {
        boolean result = false;
        if (reference.hasCitation() && reference.getCitation().hasCitationCrossReferences()) {
            result =
                    reference.getCitation().getCitationCrossReferences().stream()
                            .anyMatch(xref -> xref.getDatabase() == CitationDatabase.PUBMED);
        }
        return result;
    }

    private static boolean hasDoi(UniProtKBReference reference) {
        boolean result = false;
        if (reference.hasCitation() && reference.getCitation().hasCitationCrossReferences()) {
            result =
                    reference.getCitation().getCitationCrossReferences().stream()
                            .anyMatch(xref -> xref.getDatabase() == CitationDatabase.DOI);
        }
        return result;
    }

    private static boolean hasAgricola(UniProtKBReference reference) {
        boolean result = false;
        if (reference.hasCitation() && reference.getCitation().hasCitationCrossReferences()) {
            result =
                    reference.getCitation().getCitationCrossReferences().stream()
                            .anyMatch(xref -> xref.getDatabase() == CitationDatabase.AGRICOLA);
        }
        return result;
    }

    private static boolean noIds(UniProtKBReference reference) {
        boolean result = true;
        if (reference.hasCitation() && reference.getCitation().hasCitationCrossReferences()) {
            result = false;
        }
        return result;
    }
}
