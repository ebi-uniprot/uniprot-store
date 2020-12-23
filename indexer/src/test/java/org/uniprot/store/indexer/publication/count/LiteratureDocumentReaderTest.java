package org.uniprot.store.indexer.publication.count;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.indexer.test.config.FakeIndexerSpringBootApplication;
import org.uniprot.store.indexer.test.config.SolrTestConfig;
import org.uniprot.store.search.document.literature.LiteratureDocument;
import org.uniprot.store.search.document.publication.PublicationDocument;

/**
 * @author sahmad
 * @created 23/12/2020
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {FakeIndexerSpringBootApplication.class, SolrTestConfig.class})
public class LiteratureDocumentReaderTest {
    @Autowired private UniProtSolrClient solrClient;

    @Test
    void testRead() throws Exception {
        DocumentsStatsProcessor statsProcessor = new DocumentsStatsProcessor(solrClient);
        TypesFacetProcessor facetProcessor = new TypesFacetProcessor(solrClient);
        LiteratureDocumentReader reader = new LiteratureDocumentReader(solrClient);
        LiteratureDocument doc;
        Set<String> ids = new HashSet<>();
        int i = 1;
        while ((doc = reader.read()) != null) {
            PublicationDocument docWithStats = facetProcessor.process(doc);
            if (Objects.nonNull(docWithStats)) {
                statsProcessor.process(docWithStats);
            }
            System.out.println(i + ":" + doc.getId());
            i++;
            if (ids.contains(doc.getId())) {
                System.out.println(doc.getId() + " is there");
            }
            Assertions.assertFalse(ids.contains(doc.getId()));
            ids.add(doc.getId());
            Assertions.assertNotNull(doc);
        }
    }
}
