package uk.ac.ebi.uniprot.indexer.common.writer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.solr.core.SolrTemplate;
import org.springframework.data.solr.core.query.SimpleQuery;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.ac.ebi.uniprot.indexer.test.config.FakeIndexerSpringBootApplication;
import uk.ac.ebi.uniprot.indexer.test.config.TestConfig;
import uk.ac.ebi.uniprot.search.SolrCollection;
import uk.ac.ebi.uniprot.search.document.dbxref.CrossRefDocument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {FakeIndexerSpringBootApplication.class, TestConfig.class})
class SolrDocumentWriterTest {

    private SolrDocumentWriter solrDocumentWriter;

    @Autowired
    private SolrTemplate template;


    private static String random;

    @BeforeAll
    static void setSolrClient() throws IOException {
        random = UUID.randomUUID().toString().substring(0, 5);
    }

    @BeforeEach
    void initDocumentWriter() {
        solrDocumentWriter = new SolrDocumentWriter(template, SolrCollection.crossref);
    }

    @AfterEach
    void stopSolrClient() {
        template.delete(SolrCollection.crossref.name(), new SimpleQuery("*:*"));
        template.commit(SolrCollection.crossref.name());
    }

    @Test
    void testWriteCrossRefs() throws Exception {
        List<CrossRefDocument> dbxrefList = IntStream.range(0, 10).mapToObj(i -> createDBXRef(i))
                .collect(Collectors.toList());
        // write the cross refs to the solr
        solrDocumentWriter.write(dbxrefList);
        // get the cross refs and verify
        Page<CrossRefDocument> response = template
                .query(SolrCollection.crossref.name(), new SimpleQuery("*:*"), CrossRefDocument.class);
        assertNotNull(response);
        assertEquals(10, response.getTotalElements());
        List<CrossRefDocument> results = response.getContent();
        assertNotNull(results);
        assertEquals(dbxrefList.size(), results.size());
        results.stream().forEach(dbXref -> verifyDBXRef(dbXref));
    }

    private void verifyDBXRef(CrossRefDocument dbxRef) {
        assertNotNull(dbxRef.getAccession(), "Accession is null");
        assertNotNull(dbxRef.getAbbrev(), "Abbrev is null");
        assertNotNull(dbxRef.getName(), "Name is null");
        assertNotNull(dbxRef.getPubMedId(), "PUBMED ID is null");
        assertNotNull(dbxRef.getDoiId(), "DOI Id is null");
        assertNotNull(dbxRef.getLinkType(), "Link Type is null");
        assertNotNull(dbxRef.getServer(), "Server is null");
        assertNotNull(dbxRef.getDbUrl(), "DB URL is null");
        assertNotNull(dbxRef.getCategory(), "Category is null");
    }

    private CrossRefDocument createDBXRef(int suffix) {
        String ac = random + "-AC-" + suffix;
        String ab = random + "-AB-" + suffix;
        String nm = random + "-NM-" + suffix;
        String pb = random + "-PB-" + suffix;
        String di = random + "-DI-" + suffix;
        String lt = random + "-LT-" + suffix;
        String sr = random + "-SR-" + suffix;
        String du = random + "-DU-" + suffix;
        String ct = random + "-CT-" + suffix;
        String co = random + "-CO-" + suffix;
        List<String> contents = new ArrayList<>();
        contents.add(co);

        CrossRefDocument.CrossRefDocumentBuilder builder = CrossRefDocument.builder();
        builder.abbrev(ab).accession(ac).categoryStr(ct).dbUrl(du);
        builder.doiId(di).linkType(lt).name(nm).pubMedId(pb).server(sr);
        return builder.build();
    }
}