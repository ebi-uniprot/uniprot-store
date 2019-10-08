package indexer.uniprot;

import org.apache.spark.api.java.function.MapFunction;
import org.uniprot.core.flatfile.parser.UniprotLineParser;
import org.uniprot.core.flatfile.parser.impl.DefaultUniprotLineParserFactory;
import org.uniprot.core.flatfile.parser.impl.SupportingDataMapImpl;
import org.uniprot.core.flatfile.parser.impl.entry.EntryObject;
import org.uniprot.core.flatfile.parser.impl.entry.EntryObjectConverter;
import org.uniprot.core.uniprot.UniProtEntry;

import java.io.Serializable;

/**
 * @author lgonzales
 * @since 2019-10-01
 */
public class DatasetUniprotEntryConverter implements MapFunction<String, UniProtEntry>, Serializable {

    private static final long serialVersionUID = -1474160579218788985L;

    @Override
    public UniProtEntry call(String flatFileStringValue) throws Exception {
        UniprotLineParser<EntryObject> entryParser = new DefaultUniprotLineParserFactory().createEntryParser();
        EntryObjectConverter entryObjectConverter = new EntryObjectConverter(new SupportingDataMapImpl(), true);

        EntryObject parsed = entryParser.parse(flatFileStringValue + "\n//\n");
        return entryObjectConverter.convert(parsed);
    }

}
