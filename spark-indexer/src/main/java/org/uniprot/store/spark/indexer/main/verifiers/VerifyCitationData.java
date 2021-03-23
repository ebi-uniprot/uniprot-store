package org.uniprot.store.spark.indexer.main.verifiers;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.core.citation.CitationDatabase;
import org.uniprot.core.uniprotkb.UniProtKBReference;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;
import org.uniprot.store.spark.indexer.main.verifiers.mapper.UniProtKBPublicationMapper;
import org.uniprot.store.spark.indexer.uniprot.UniProtKBRDDTupleReader;
import scala.Tuple2;

import java.util.Map;
import java.util.ResourceBundle;

/**
 * @author lgonzales
 * @since 22/03/2021
 */
@Slf4j
public class VerifyCitationData {

    public static void main(String[] args) {
        ResourceBundle applicationConfig = SparkUtils.loadApplicationProperty();
        JavaSparkContext sparkContext = SparkUtils.loadSparkContext(applicationConfig);

        JobParameter jobParameter =
                JobParameter.builder()
                        .applicationConfig(applicationConfig)
                        .releaseName(args[0])
                        .sparkContext(sparkContext)
                        .build();

        JavaRDD<String> solrInputDocumentRDD =
                new UniProtKBRDDTupleReader(jobParameter,false).loadFlatFileToRDD();
/*

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
*/

        log.info("------------------------------------------------------------------------------");
        Map<String, Long> noIdsTypeCount = solrInputDocumentRDD
                .flatMap(new UniProtKBPublicationMapper())
                .filter(VerifyCitationData::noIds)
                .map(VerifyCitationData::mapByType)
                .countByValue();
        noIdsTypeCount.forEach((key, value) -> log.info("noIdsTypeCount: Type: {} , count: {} ", key, value));

        log.info("-------------------------------------------------------------------------------");
        Map<String, Long> idsTypeCount = solrInputDocumentRDD
                .flatMap(new UniProtKBPublicationMapper())
                .filter(VerifyCitationData::hasId)
                .map(VerifyCitationData::mapByType)
                .countByValue();
        idsTypeCount.forEach((key, value) -> log.info("idsTypeCount: Type: {} , count: {} ", key, value));
        log.info("--------------------------------------------------------------------------------");
        sparkContext.close();
    }

    private static boolean hasId(UniProtKBReference reference) {
        boolean result = false;
        if(reference.hasCitation() && reference.getCitation().hasCitationCrossReferences()) {
            result = true;
        }
        return result;
    }

    private static String mapByType(UniProtKBReference reference) {
        String result = "NO-CITATION";
        if(reference.hasCitation()){
            result = reference.getCitation().getCitationType().name();
        }
        return result;
    }

    private static boolean hasPubmedId(UniProtKBReference reference) {
        boolean result = false;
        if(reference.hasCitation() && reference.getCitation().hasCitationCrossReferences()) {
            result = reference
                    .getCitation()
                    .getCitationCrossReferences().stream()
                    .anyMatch(xref -> xref.getDatabase() == CitationDatabase.PUBMED);
        }
        return result;
    }

    private static boolean hasDoi(UniProtKBReference reference) {
        boolean result = false;
        if(reference.hasCitation() && reference.getCitation().hasCitationCrossReferences()) {
            result = reference
                    .getCitation()
                    .getCitationCrossReferences().stream()
                    .anyMatch(xref -> xref.getDatabase() == CitationDatabase.DOI);
        }
        return result;
    }

    private static boolean hasAgricola(UniProtKBReference reference) {
        boolean result = false;
        if(reference.hasCitation() && reference.getCitation().hasCitationCrossReferences()) {
            result = reference
                    .getCitation()
                    .getCitationCrossReferences().stream()
                    .anyMatch(xref -> xref.getDatabase() == CitationDatabase.AGRICOLA);
        }
        return result;
    }

    private static boolean noIds(UniProtKBReference reference) {
        boolean result = true;
        if(reference.hasCitation() && reference.getCitation().hasCitationCrossReferences()) {
            result = false;
        }
        return result;
    }
}
