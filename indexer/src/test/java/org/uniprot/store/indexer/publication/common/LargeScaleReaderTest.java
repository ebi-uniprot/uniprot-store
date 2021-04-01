package org.uniprot.store.indexer.publication.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.uniprot.core.publication.MappedReferenceType;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.indexer.publication.PublicationITUtil;
import org.uniprot.store.indexer.test.config.FakeIndexerSpringBootApplication;
import org.uniprot.store.indexer.test.config.SolrTestConfig;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.literature.LiteratureDocument;

/**
 * @author lgonzales
 * @since 14/01/2021
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {FakeIndexerSpringBootApplication.class, SolrTestConfig.class})
class LargeScaleReaderTest {

    @Autowired private UniProtSolrClient solrClient;

    @AfterEach
    void cleanSolrData() {
        solrClient.delete(SolrCollection.literature, "*:*");
        solrClient.commit(SolrCollection.literature);
    }

    @Test
    void canReadEmptyLiteratureReturnNull() {
        LargeScaleReader reader =
                new LargeScaleReader(solrClient, LargeScaleSolrFieldQuery.COMMUNITY);
        Assertions.assertNull(reader.read());
    }

    @Test
    void canReadSuccessOneSolrPage() throws Exception {
        for (int pubmedId = 1; pubmedId <= 10; pubmedId++) {
            LiteratureDocument litDoc = PublicationITUtil.createLargeScaleLiterature(pubmedId);
            solrClient.saveBeans(SolrCollection.literature, Collections.singleton(litDoc));
        }
        solrClient.commit(SolrCollection.literature);

        LargeScaleReader reader =
                new LargeScaleReader(solrClient, LargeScaleSolrFieldQuery.COMMUNITY);
        Set<String> result = reader.read();
        assertNotNull(result);
        assertEquals(10, result.size());
        assertTrue(result.contains("1"));
        assertTrue(result.contains("10"));

        // bring everything in one read
        Assertions.assertNull(reader.read());
    }

    @ParameterizedTest
    @EnumSource(MappedReferenceType.class)
    void testLargeScaleWithComputationalCountOnly(MappedReferenceType type) throws Exception {
        int pubmedId = ThreadLocalRandom.current().nextInt();
        LiteratureDocument litDoc =
                PublicationITUtil.createLargeScaleLiteratureWithOneCount(pubmedId, type);
        solrClient.saveBeans(SolrCollection.literature, Collections.singleton(litDoc));
        solrClient.commit(SolrCollection.literature);

        LargeScaleSolrFieldQuery fieldName = getFieldName(type);

        LargeScaleReader reader = new LargeScaleReader(solrClient, fieldName);
        Set<String> result = reader.read();
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.contains(String.valueOf(pubmedId)));
        // bring everything in one read
        Assertions.assertNull(reader.read());
    }

    @Test
    void canReadSuccessTwoSolrPages() throws Exception {
        for (int pubmedId = 1; pubmedId <= 250; pubmedId++) {
            LiteratureDocument litDoc = PublicationITUtil.createLargeScaleLiterature(pubmedId);
            solrClient.saveBeans(SolrCollection.literature, Collections.singleton(litDoc));
        }
        solrClient.commit(SolrCollection.literature);

        LargeScaleReader reader =
                new LargeScaleReader(solrClient, LargeScaleSolrFieldQuery.COMMUNITY);
        Set<String> result = reader.read();
        assertNotNull(result);
        assertEquals(250, result.size());
        assertTrue(result.contains("1"));
        assertTrue(result.contains("250"));

        // bring everything in one read
        Assertions.assertNull(reader.read());
    }

    private LargeScaleSolrFieldQuery getFieldName(MappedReferenceType type) {
        LargeScaleSolrFieldQuery fieldName;
        if (type == MappedReferenceType.COMMUNITY) {
            fieldName = LargeScaleSolrFieldQuery.COMMUNITY;
        } else if (type == MappedReferenceType.COMPUTATIONAL) {
            fieldName = LargeScaleSolrFieldQuery.COMPUTATIONAL;
        } else {
            fieldName = LargeScaleSolrFieldQuery.UNIPROT_KB;
        }
        return fieldName;
    }
}
