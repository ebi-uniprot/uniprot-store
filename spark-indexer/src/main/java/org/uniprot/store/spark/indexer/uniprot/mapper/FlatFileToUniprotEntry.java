package org.uniprot.store.spark.indexer.uniprot.mapper;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.core.flatfile.parser.SupportingDataMap;
import org.uniprot.core.flatfile.parser.UniprotkbLineParser;
import org.uniprot.core.flatfile.parser.impl.DefaultUniprotkbLineParserFactory;
import org.uniprot.core.flatfile.parser.impl.entry.EntryObject;
import org.uniprot.core.flatfile.parser.impl.entry.EntryObjectConverter;
import org.uniprot.core.uniprotkb.UniProtkbEntry;

import scala.Serializable;
import scala.Tuple2;

/**
 * This mapper convert flat file entry in String format to a tuple of Tuple2{key=accession,
 * value={@link UniProtkbEntry}}
 *
 * @author lgonzales
 * @since 2019-11-12
 */
@Slf4j
public class FlatFileToUniprotEntry
        implements PairFunction<String, String, UniProtkbEntry>, Serializable {
    private static final long serialVersionUID = 8571366803867491177L;

    private final SupportingDataMap supportingDataMap;

    public FlatFileToUniprotEntry(SupportingDataMap supportingDataMap) {
        this.supportingDataMap = supportingDataMap;
    }

    /**
     * @param entryString flat file entry in String format
     * @return Tuple2{key=accession, value={@link UniProtkbEntry}}
     */
    @Override
    public Tuple2<String, UniProtkbEntry> call(String entryString) throws Exception {
        UniprotkbLineParser<EntryObject> entryParser =
                new DefaultUniprotkbLineParserFactory().createEntryParser();
        EntryObjectConverter entryObjectConverter =
                new EntryObjectConverter(supportingDataMap, false);

        EntryObject parsed = entryParser.parse(entryString);
        UniProtkbEntry uniProtkbEntry = entryObjectConverter.convert(parsed);
        return new Tuple2<>(uniProtkbEntry.getPrimaryAccession().getValue(), uniProtkbEntry);
    }
}
