package org.uniprot.store.spark.indexer.uniprot.mapper;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.core.flatfile.parser.SupportingDataMap;
import org.uniprot.core.flatfile.parser.UniprotKBLineParser;
import org.uniprot.core.flatfile.parser.impl.DefaultUniprotKBLineParserFactory;
import org.uniprot.core.flatfile.parser.impl.entry.EntryObject;
import org.uniprot.core.flatfile.parser.impl.entry.EntryObjectConverter;
import org.uniprot.core.uniprotkb.UniProtKBEntry;

import org.uniprot.cv.FileParseException;
import scala.Serializable;
import scala.Tuple2;

/**
 * This mapper convert flat file entry in String format to a tuple of Tuple2{key=accession,
 * value={@link UniProtKBEntry}}
 *
 * @author lgonzales
 * @since 2019-11-12
 */
@Slf4j
public class FlatFileToUniprotEntry
        implements PairFunction<String, String, UniProtKBEntry>, Serializable {
    private static final long serialVersionUID = 8571366803867491177L;

    private final SupportingDataMap supportingDataMap;

    public FlatFileToUniprotEntry(SupportingDataMap supportingDataMap) {
        this.supportingDataMap = supportingDataMap;
    }

    /**
     * @param entryString flat file entry in String format
     * @return Tuple2{key=accession, value={@link UniProtKBEntry}}
     */
    @Override
    public Tuple2<String, UniProtKBEntry> call(String entryString) throws Exception {
        try {
            UniprotKBLineParser<EntryObject> entryParser =
                    new DefaultUniprotKBLineParserFactory().createEntryParser();
            EntryObjectConverter entryObjectConverter =
                    new EntryObjectConverter(supportingDataMap, false);
            EntryObject parsed = entryParser.parse(entryString);
            UniProtKBEntry uniProtkbEntry = entryObjectConverter.convert(parsed);
            return new Tuple2<>(uniProtkbEntry.getPrimaryAccession().getValue(), uniProtkbEntry);
        } catch (Exception e){
            throw new FileParseException("Unable to parse entry: \n"+entryString, e);
        }
    }
}
