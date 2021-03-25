package org.uniprot.store.spark.indexer.main.verifiers;

import java.util.List;
import java.util.ResourceBundle;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.uniprot.core.citation.Citation;
import org.uniprot.core.citation.CitationDatabase;
import org.uniprot.core.citation.impl.*;
import org.uniprot.core.uniprotkb.UniProtKBReference;
import org.uniprot.core.util.Pair;
import org.uniprot.core.util.PairImpl;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;
import org.uniprot.store.spark.indexer.literature.mapper.LiteratureUniProtKBReferencesMapper;
import org.uniprot.store.spark.indexer.uniprot.UniProtKBRDDTupleReader;

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
                new UniProtKBRDDTupleReader(jobParameter, false).loadFlatFileToRDD();

        List<Pair<Citation, Citation>> result = solrInputDocumentRDD
                .flatMapToPair(new LiteratureUniProtKBReferencesMapper())
                .filter(ref -> !hasPubmedId(ref._2))
                .mapValues(ref -> ref.getCitation())
                .repartition(30000)
                .groupByKey()
                .repartition(30000)
                .mapValues(new AreEquals())
                .filter(tuple2 -> tuple2._2 != null && tuple2._2.getValue() != null)
                .values()
                .collect();
        log.info("Duplicated Hash: {}", result.size());
        result.forEach(pair -> {
            log.info("----------------------");
            log.info("key: {}", pair.getKey());
            log.info("val: {}", pair.getValue());
            switch (pair.getKey().getCitationType()){
                case BOOK:
                    BookImpl book = (BookImpl) pair.getKey();
                    log.info("kha: {}", book.getHashInput());
                    BookImpl boov = (BookImpl) pair.getValue();
                    log.info("vha: {}", boov.getHashInput());
                    break;
                case PATENT:
                    PatentImpl pa = (PatentImpl) pair.getKey();
                    log.info("kha: {}", pa.getHashInput());
                    PatentImpl pav = (PatentImpl) pair.getValue();
                    log.info("vha: {}", pav.getHashInput());
                    break;
                case THESIS:
                    ThesisImpl th = (ThesisImpl) pair.getKey();
                    log.info("kha: {}", th.getHashInput());
                    ThesisImpl thv = (ThesisImpl) pair.getValue();
                    log.info("vha: {}", thv.getHashInput());
                    break;
                case SUBMISSION:
                    SubmissionImpl sub = (SubmissionImpl) pair.getKey();
                    log.info("kha: {}", sub.getHashInput());
                    SubmissionImpl subv = (SubmissionImpl) pair.getValue();
                    log.info("vha: {}", subv.getHashInput());
                    break;
                case ELECTRONIC_ARTICLE:
                    ElectronicArticleImpl ea = (ElectronicArticleImpl) pair.getKey();
                    log.info("kha: {}", ea.getHashInput());
                    ElectronicArticleImpl eav = (ElectronicArticleImpl) pair.getValue();
                    log.info("vha: {}", eav.getHashInput());
                    break;
                default:
                    log.info("No print");
            }


        });
        log.info("The End");

        /*                long withDoi = solrInputDocumentRDD
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

    private static class AreEquals implements Function<Iterable<Citation>, Pair<Citation, Citation>> {

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
