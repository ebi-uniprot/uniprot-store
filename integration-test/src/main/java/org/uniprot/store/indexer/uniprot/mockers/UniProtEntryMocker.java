package org.uniprot.store.indexer.uniprot.mockers;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.uniprot.core.flatfile.parser.UniProtParser;
import org.uniprot.core.flatfile.parser.impl.DefaultUniProtParser;
import org.uniprot.core.flatfile.parser.impl.SupportingDataMapImpl;
import org.uniprot.core.impl.SequenceBuilder;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.core.uniprotkb.UniProtKBEntryType;
import org.uniprot.core.uniprotkb.impl.UniProtKBAccessionBuilder;
import org.uniprot.core.uniprotkb.impl.UniProtKBEntryBuilder;

/**
 * Created 19/09/18
 *
 * @author Edd
 */
public class UniProtEntryMocker {

    public enum Type {
        SP("Q8DIA7.dat"),
        SP_COMPLEX("P97929.dat"),
        TR("F1Q0X3.dat"),
        SP_CANONICAL("P21802.dat"),
        SP_ISOFORM("P21802-2.dat"),
        SP_CANONICAL_ISOFORM("P21802-1.dat"),
        WITH_DEMERGED_SEC_ACCESSION("P63150.dat");

        private final String fileName;

        Type(String fileName) {
            this.fileName = fileName;
        }
    }

    private static Map<Type, UniProtKBEntry> entryMap = new HashMap<>();

    static {
        for (Type type : Type.values()) {
            InputStream is =
                    UniProtEntryMocker.class.getResourceAsStream("/entry/" + type.fileName);
            try {
                UniProtParser parser = new DefaultUniProtParser(new SupportingDataMapImpl(), true);
                UniProtKBEntry entry = parser.parse(IOUtils.toString(is, Charset.defaultCharset()));
                entryMap.put(type, entry);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
    }

    public static UniProtKBEntry create(String accession) {
        UniProtKBEntry entry = entryMap.get(Type.SP);
        UniProtKBEntryBuilder builder = UniProtKBEntryBuilder.from(entry);
        return builder.primaryAccession(new UniProtKBAccessionBuilder(accession).build())
                .entryType(UniProtKBEntryType.TREMBL)
                .sequence(new SequenceBuilder("AAAAA").build())
                .build();
    }

    public static UniProtKBEntry create(Type type) {
        return UniProtKBEntryBuilder.from(entryMap.get(type)).build();
    }

    public static Collection<UniProtKBEntry> createEntries() {
        return entryMap.values();
    }
}
