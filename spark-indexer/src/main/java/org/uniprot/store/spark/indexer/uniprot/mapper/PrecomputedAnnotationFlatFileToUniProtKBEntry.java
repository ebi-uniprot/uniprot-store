package org.uniprot.store.spark.indexer.uniprot.mapper;

import org.apache.spark.api.java.function.Function;
import org.uniprot.core.flatfile.parser.SupportingDataMap;
import org.uniprot.core.flatfile.parser.UniprotKBLineParser;
import org.uniprot.core.flatfile.parser.impl.aaentry.AAEntryObject;
import org.uniprot.core.flatfile.parser.impl.aaentry.AAEntryObjectConverter;
import org.uniprot.core.flatfile.parser.impl.aaentry.DefaultAAUniProtKBLineParserFactory;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.store.spark.indexer.common.exception.SparkIndexException;

import lombok.extern.slf4j.Slf4j;
import scala.Serializable;

@Slf4j
public class PrecomputedAnnotationFlatFileToUniProtKBEntry
        implements Function<String, UniProtKBEntry>, Serializable {

    private static final long serialVersionUID = -3752802923317859249L;
    private final SupportingDataMap supportingDataMap;

    public PrecomputedAnnotationFlatFileToUniProtKBEntry(SupportingDataMap supportingDataMap) {
        this.supportingDataMap = supportingDataMap;
    }

    @Override
    public UniProtKBEntry call(String entryString) throws Exception {
        UniprotKBLineParser<AAEntryObject> entryParser =
                new DefaultAAUniProtKBLineParserFactory().createEntryParser();
        AAEntryObjectConverter entryObjectConverter =
                new AAEntryObjectConverter(supportingDataMap, true);
        try {
            AAEntryObject parsed = entryParser.parse(entryString);
            UniProtKBEntry uniProtkbEntry = entryObjectConverter.convert(parsed);
            return uniProtkbEntry;
        } catch (Exception e) {
            throw new SparkIndexException("Error parsing: \n" + entryString, e);
        }
    }
}
