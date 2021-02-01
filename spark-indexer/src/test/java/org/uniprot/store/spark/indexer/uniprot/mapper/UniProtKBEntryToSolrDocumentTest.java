package org.uniprot.store.spark.indexer.uniprot.mapper;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.uniprot.core.flatfile.parser.SupportingDataMap;
import org.uniprot.core.flatfile.parser.UniprotKBLineParser;
import org.uniprot.core.flatfile.parser.impl.DefaultUniprotKBLineParserFactory;
import org.uniprot.core.flatfile.parser.impl.entry.EntryObject;
import org.uniprot.core.flatfile.parser.impl.entry.EntryObjectConverter;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.core.uniprotkb.UniProtKBEntryType;
import org.uniprot.core.uniprotkb.impl.UniProtKBEntryBuilder;
import org.uniprot.store.search.document.DocumentConversionException;
import org.uniprot.store.search.document.uniprot.UniProtDocument;
import org.uniprot.store.spark.indexer.uniprot.converter.SupportingDataMapHDSFImpl;

/**
 * @author lgonzales
 * @since 2019-11-13
 */
class UniProtKBEntryToSolrDocumentTest {

    @Test
    void testMapUniprotEntryToDocument() throws Exception {
        SupportingDataMap supportingDataMap = new SupportingDataMapHDSFImpl(null, null, null, null);
        UniprotKBLineParser<EntryObject> entryParser =
                new DefaultUniprotKBLineParserFactory().createEntryParser();
        EntryObjectConverter entryObjectConverter =
                new EntryObjectConverter(supportingDataMap, true);

        List<String> flatFileLines =
                Files.readAllLines(
                        Paths.get(
                                ClassLoader.getSystemResource("2020_02/uniprotkb/Q9EPI6.sp")
                                        .toURI()));
        EntryObject parsed = entryParser.parse(String.join("\n", flatFileLines));
        UniProtKBEntry uniProtkbEntry = entryObjectConverter.convert(parsed);

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
        UniProtKBEntry entry =
                new UniProtKBEntryBuilder("P12345", "ID_P12345", UniProtKBEntryType.SWISSPROT)
                        .build();
        assertThrows(
                DocumentConversionException.class,
                () -> mapper.call(entry),
                "Error converting UniProt entry");
    }
}
