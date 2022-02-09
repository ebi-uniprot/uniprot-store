package org.uniprot.store.spark.indexer.uniprot.mapper;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.uniprot.core.cv.subcell.SubcellularLocationEntry;
import org.uniprot.store.search.document.uniprot.UniProtDocument;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;
import org.uniprot.store.spark.indexer.subcell.SubcellularLocationRDDReader;
import org.uniprot.store.spark.indexer.uniprot.UniProtKBDocumentsToHDFSWriter;
import org.uniprot.store.spark.indexer.uniprot.UniProtKBRDDTupleReader;

import scala.Tuple2;

/**
 * @author sahmad
 * @created 09/02/2022
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class UniProtDocumentSubcellEntriesMapperTest {

    private JobParameter parameter;

    @BeforeAll
    void setUpWriter() {
        ResourceBundle application = SparkUtils.loadApplicationProperty();
        JavaSparkContext sparkContext = SparkUtils.loadSparkContext(application);
        parameter =
                JobParameter.builder()
                        .applicationConfig(application)
                        .releaseName("2020_02")
                        .sparkContext(sparkContext)
                        .build();
    }

    @AfterAll
    void closeWriter() {
        parameter.getSparkContext().close();
    }

    @Test
    void testMergeAncestorsSubcellularLocation() throws Exception {
        UniProtKBDocumentsToHDFSWriter writer = new UniProtKBDocumentsToHDFSWriter(parameter);
        UniProtKBRDDTupleReader reader = new UniProtKBRDDTupleReader(parameter, false);
        UniProtDocument uniProtDocument =
                reader.load()
                        .mapValues(new UniProtEntryToSolrDocument(new HashMap<>()))
                        .values()
                        .first();
        Set<String> oldContent = new HashSet<>(uniProtDocument.content);
        List<String> slIds =
                uniProtDocument.content.stream()
                        .filter(str -> str.startsWith("SL-"))
                        .collect(Collectors.toList());
        Set<String> slTerms = new HashSet<>(uniProtDocument.subcellLocationTerm);
        SubcellularLocationRDDReader slReader = new SubcellularLocationRDDReader(parameter);
        Map<String, SubcellularLocationEntry> slIdEntryMap = slReader.load().collectAsMap();
        List<SubcellularLocationEntry> slEntries =
                slIds.stream().map(slId -> slIdEntryMap.get(slId)).collect(Collectors.toList());

        UniProtDocumentSubcellEntriesMapper mapper = new UniProtDocumentSubcellEntriesMapper();
        var tuple =
                new Tuple2<UniProtDocument, Optional<Iterable<SubcellularLocationEntry>>>(
                        uniProtDocument, Optional.of(slEntries));
        UniProtDocument docWithAncestorsSl = mapper.call(tuple);
        Assertions.assertNotNull(docWithAncestorsSl);
        Assertions.assertTrue(docWithAncestorsSl.content.size() > oldContent.size());
        List<String> newSlIds =
                docWithAncestorsSl.content.stream()
                        .filter(str -> str.startsWith("SL-"))
                        .collect(Collectors.toList());
        Assertions.assertTrue(docWithAncestorsSl.content.containsAll(slIds));
        Assertions.assertTrue(newSlIds.size() > slIds.size());
        Assertions.assertTrue(docWithAncestorsSl.subcellLocationTerm.size() > slTerms.size());
        Assertions.assertTrue(docWithAncestorsSl.subcellLocationTerm.containsAll(slTerms));
    }

    @Test
    void testPopulateSubcellWithEmptyList() throws Exception {
        UniProtDocumentSubcellEntriesMapper mapper = new UniProtDocumentSubcellEntriesMapper();
        UniProtDocument document = new UniProtDocument();
        var tuple =
                new Tuple2<UniProtDocument, Optional<Iterable<SubcellularLocationEntry>>>(
                        document, Optional.empty());
        UniProtDocument resultDoc = mapper.call(tuple);
        Assertions.assertNotNull(resultDoc);
        Assertions.assertTrue(resultDoc.content.isEmpty());
        Assertions.assertTrue(resultDoc.subcellLocationTerm.isEmpty());
    }
}
