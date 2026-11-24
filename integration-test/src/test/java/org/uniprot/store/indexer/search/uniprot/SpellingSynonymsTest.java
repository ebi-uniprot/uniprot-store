package org.uniprot.store.indexer.search.uniprot;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPathFactory;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

class SpellingSynonymsTest {
    private static final Path CONF =
            Path.of("../index-config/src/main/solr-config/uniprot-collections/uniprot/conf");

    @ParameterizedTest
    @ValueSource(strings = {"content", "protgene_default", "protein_name"})
    void expandsBothSpellingsWithoutExpandingUnrelatedTerms(String field) throws Exception {
        try (Analyzer indexAnalyzer = analyzer(field, "index");
                Analyzer queryAnalyzer = analyzer(field, "query");
                ByteBuffersDirectory directory = new ByteBuffersDirectory()) {
            try (IndexWriter writer =
                    new IndexWriter(directory, new IndexWriterConfig(indexAnalyzer))) {
                for (String value :
                        new String[] {
                            "haemoglobin alpha", "hemoglobin alpha", "hemoglobin beta",
                            "factor 1", "factor 9", "mitochondrion"
                        }) {
                    Document document = new Document();
                    document.add(new TextField(field, value, Field.Store.NO));
                    writer.addDocument(document);
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                QueryParser parser = new QueryParser(field, queryAnalyzer);
                assertEquals(parser.parse("haemoglobin"), parser.parse("hemoglobin"));
                assertEquals(3, searcher.count(parser.parse("haemoglobin")));
                assertEquals(3, searcher.count(parser.parse("HEMOGLOBIN")));
                assertEquals(2, searcher.count(parser.parse("\"haemoglobin alpha\"")));
                assertEquals(2, searcher.count(parser.parse("\"hemoglobin-alpha\"")));
                assertEquals(0, searcher.count(parser.parse("\"alpha haemoglobin\"")));
                assertEquals(1, searcher.count(parser.parse("\"factor 1\"")));
                assertEquals(0, searcher.count(parser.parse("\"factor i\"")));
                assertEquals(0, searcher.count(parser.parse("\"factor nice\"")));
                assertEquals(0, searcher.count(parser.parse("mitochondrial")));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"content", "protgene_default", "protein_name"})
    void indexAnalyzerDoesNotExpandSpellings(String field) throws Exception {
        try (Analyzer indexAnalyzer = analyzer(field, "index");
                ByteBuffersDirectory directory = new ByteBuffersDirectory()) {
            try (IndexWriter writer =
                    new IndexWriter(directory, new IndexWriterConfig(indexAnalyzer))) {
                Document document = new Document();
                document.add(new TextField(field, "hemoglobin", Field.Store.NO));
                writer.addDocument(document);
            }
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                QueryParser parser = new QueryParser(field, indexAnalyzer);
                IndexSearcher searcher = new IndexSearcher(reader);
                assertEquals(1, searcher.count(parser.parse("hemoglobin")));
                assertEquals(0, searcher.count(parser.parse("haemoglobin")));
            }
        }
    }

    private Analyzer analyzer(String field, String type) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        org.w3c.dom.Document schema =
                factory.newDocumentBuilder().parse(CONF.resolve("schema.xml").toFile());
        String fieldType =
                XPathFactory.newInstance()
                        .newXPath()
                        .evaluate("/schema/field[@name='" + field + "']/@type", schema);
        Node chain =
                (Node)
                        XPathFactory.newInstance()
                                .newXPath()
                                .evaluate(
                                        "/schema/fieldType[@name='"
                                                + fieldType
                                                + "']/analyzer[@type='"
                                                + type
                                                + "']",
                                        schema,
                                        javax.xml.xpath.XPathConstants.NODE);
        CustomAnalyzer.Builder builder = CustomAnalyzer.builder(CONF);
        for (Node node = chain.getFirstChild(); node != null; node = node.getNextSibling()) {
            if (!(node instanceof Element)) {
                continue;
            }
            Map<String, String> args = new HashMap<>();
            NamedNodeMap attributes = node.getAttributes();
            for (int i = 0; i < attributes.getLength(); i++) {
                Node attribute = attributes.item(i);
                args.put(attribute.getNodeName(), attribute.getNodeValue());
            }
            String name = args.remove("class").replace("solr.", "").replace("Factory", "");
            if (node.getNodeName().equals("tokenizer")) {
                builder.withTokenizer(name.replace("Tokenizer", ""), args);
            } else {
                builder.addTokenFilter(name.replace("Filter", ""), args);
            }
        }
        return builder.build();
    }
}
