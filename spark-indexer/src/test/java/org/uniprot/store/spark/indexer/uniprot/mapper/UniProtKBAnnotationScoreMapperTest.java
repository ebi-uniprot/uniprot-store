package org.uniprot.store.spark.indexer.uniprot.mapper;

import static org.junit.jupiter.api.Assertions.*;

import java.io.InputStream;
import java.nio.charset.Charset;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.uniprot.core.flatfile.parser.SupportingDataMap;
import org.uniprot.core.flatfile.parser.impl.DefaultUniProtParser;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.core.uniprotkb.UniProtKBEntryType;
import org.uniprot.core.uniprotkb.impl.UniProtKBEntryBuilder;
import org.uniprot.store.spark.indexer.uniprot.converter.SupportingDataMapHDSFImpl;

/**
 * @author lgonzales
 * @since 30/07/2020
 */
class UniProtKBAnnotationScoreMapperTest {

    @Test
    void testMinimalProtein() throws Exception {
        UniProtKBEntry entry =
                new UniProtKBEntryBuilder("P12345", "P12345_HOMO", UniProtKBEntryType.SWISSPROT)
                        .build();
        assertNotNull(entry);
        assertEquals(0.0d, entry.getAnnotationScore());

        UniProtKBAnnotationScoreMapper mapper = new UniProtKBAnnotationScoreMapper();
        entry = mapper.call(entry);
        assertEquals(1.0d, entry.getAnnotationScore());
    }

    @Test
    void testQ9EPI6Protein() throws Exception {
        UniProtKBEntry entry = getEntryFromFile();
        assertNotNull(entry);
        assertEquals(0.0d, entry.getAnnotationScore());

        UniProtKBAnnotationScoreMapper mapper = new UniProtKBAnnotationScoreMapper();
        entry = mapper.call(entry);
        assertEquals(5.0d, entry.getAnnotationScore());
    }

    private UniProtKBEntry getEntryFromFile() throws Exception {
        InputStream is =
                UniProtKBAnnotationScoreMapperTest.class
                        .getClassLoader()
                        .getResourceAsStream("2020_02/uniprotkb/Q9EPI6.sp");
        assertNotNull(is);
        SupportingDataMap supportingDataMap =
                new SupportingDataMapHDSFImpl(
                        "2020_02/keyword/keywlist.txt",
                        "2020_02/disease/humdisease.txt",
                        "2020_02/subcell/subcell.txt",
                        null);
        DefaultUniProtParser parser = new DefaultUniProtParser(supportingDataMap, false);
        return parser.parse(IOUtils.toString(is, Charset.defaultCharset()));
    }
}
