package org.uniprot.store.indexer.publication.common;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.uniprot.store.indexer.common.config.UniProtSolrClient;
import org.uniprot.store.indexer.publication.PublicationITUtil;
import org.uniprot.store.indexer.test.config.FakeIndexerSpringBootApplication;
import org.uniprot.store.indexer.test.config.SolrTestConfig;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.literature.LiteratureDocument;

import java.util.Collections;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author lgonzales
 * @since 14/01/2021
 */

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {FakeIndexerSpringBootApplication.class, SolrTestConfig.class})
class LargeScaleReaderTest {

    @Autowired
    private UniProtSolrClient solrClient;

    @AfterEach
    void cleanSolrData(){
        solrClient.delete(SolrCollection.literature, "*:*");
        solrClient.commit(SolrCollection.literature);
    }

    @Test
    void canReadEmptyLiteratureReturnNull(){
        LargeScaleReader reader = new LargeScaleReader(solrClient, LargeScaleSolrFieldName.COMMUNITY);
        Assertions.assertNull(reader.read());
    }

    @Test
    void canReadSuccessOneSolrPage() throws Exception{
        for(int pubmedId=1; pubmedId <= 10; pubmedId++){
            LiteratureDocument litDoc = PublicationITUtil.createLargeScaleLiterature(pubmedId);
            solrClient.saveBeans(SolrCollection.literature, Collections.singleton(litDoc));
        }
        solrClient.commit(SolrCollection.literature);

        LargeScaleReader reader = new LargeScaleReader(solrClient, LargeScaleSolrFieldName.COMMUNITY);
        Set<String> result = reader.read();
        assertNotNull(result);
        assertEquals(10, result.size());
        assertTrue(result.contains("1"));
        assertTrue(result.contains("10"));

        //bring everything in one read
        Assertions.assertNull(reader.read());
    }

    @Test
    void canReadSuccessTwoSolrPages() throws Exception{
        for(int pubmedId=1; pubmedId <= 250; pubmedId++){
            LiteratureDocument litDoc = PublicationITUtil.createLargeScaleLiterature(pubmedId);
            solrClient.saveBeans(SolrCollection.literature, Collections.singleton(litDoc));
        }
        solrClient.commit(SolrCollection.literature);

        LargeScaleReader reader = new LargeScaleReader(solrClient, LargeScaleSolrFieldName.COMMUNITY);
        Set<String> result = reader.read();
        assertNotNull(result);
        assertEquals(250, result.size());
        assertTrue(result.contains("1"));
        assertTrue(result.contains("250"));

        //bring everything in one read
        Assertions.assertNull(reader.read());
    }

}