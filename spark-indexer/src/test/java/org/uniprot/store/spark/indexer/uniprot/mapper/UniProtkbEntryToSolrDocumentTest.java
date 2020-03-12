package org.uniprot.store.spark.indexer.uniprot.mapper;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.uniprot.core.flatfile.parser.SupportingDataMap;
import org.uniprot.core.flatfile.parser.UniprotkbLineParser;
import org.uniprot.core.flatfile.parser.impl.DefaultUniprotkbLineParserFactory;
import org.uniprot.core.flatfile.parser.impl.entry.EntryObject;
import org.uniprot.core.flatfile.parser.impl.entry.EntryObjectConverter;
import org.uniprot.core.uniprotkb.UniProtkbEntry;
import org.uniprot.core.uniprotkb.UniProtkbEntryType;
import org.uniprot.core.uniprotkb.impl.UniProtkbEntryBuilder;
import org.uniprot.store.job.common.DocumentConversionException;
import org.uniprot.store.search.document.uniprot.UniProtDocument;
import org.uniprot.store.spark.indexer.uniprot.converter.SupportingDataMapHDSFImpl;

/**
 * @author lgonzales
 * @since 2019-11-13
 */
class UniProtkbEntryToSolrDocumentTest {

    @Test
    void testMapUniprotEntryToDocument() throws Exception {
        SupportingDataMap supportingDataMap = new SupportingDataMapHDSFImpl(null, null, null, null);
        UniprotkbLineParser<EntryObject> entryParser =
                new DefaultUniprotkbLineParserFactory().createEntryParser();
        EntryObjectConverter entryObjectConverter =
                new EntryObjectConverter(supportingDataMap, true);

        List<String> flatFileLines =
                Files.readAllLines(
                        Paths.get(ClassLoader.getSystemResource("uniprotkb/Q9EPI6.sp").toURI()));
        EntryObject parsed = entryParser.parse(String.join("\n", flatFileLines));
        UniProtkbEntry uniProtkbEntry = entryObjectConverter.convert(parsed);

        UniProtEntryToSolrDocument mapper = new UniProtEntryToSolrDocument(new HashMap<>());
        UniProtDocument doc = mapper.call(uniProtkbEntry);

        assertNotNull(doc);
        assertEquals("Q9EPI6", doc.accession);
        // Document converter has its own test, here we just make sure that the mapper is working as
        // expected..
    }

    @Test
    void testInvalidUniprotEntry() throws Exception {
        UniProtEntryToSolrDocument mapper = new UniProtEntryToSolrDocument(new HashMap<>());
        assertThrows(
                DocumentConversionException.class,
                () -> {
                    mapper.call(
                            new UniProtkbEntryBuilder(
                                            "P12345", "ID_P12345", UniProtkbEntryType.SWISSPROT)
                                    .build());
                },
                "Error converting UniProt entry");
    }
}
