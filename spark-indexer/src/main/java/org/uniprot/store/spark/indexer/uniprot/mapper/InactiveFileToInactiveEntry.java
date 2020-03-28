package org.uniprot.store.spark.indexer.uniprot.mapper;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.core.uniprotkb.InactiveReasonType;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.core.uniprotkb.impl.EntryInactiveReasonBuilder;
import org.uniprot.core.uniprotkb.impl.UniProtKBEntryBuilder;
import org.uniprot.core.util.Utils;

import scala.Serializable;
import scala.Tuple2;

/**
 * This class map a csv string line of an Inactive Entry To a Tuple2{key=accession, value={@link
 * UniProtKBEntry}}
 *
 * @author lgonzales
 * @since 2019-12-02
 */
@Slf4j
public class InactiveFileToInactiveEntry
        implements PairFunction<String, String, UniProtKBEntry>, Serializable {

    private static final long serialVersionUID = 8571366803867491177L;

    /**
     * @param line csv file line of an Inactive Entry in String format
     * @return Tuple2{key=accession, value={@link UniProtKBEntry}}
     */
    @Override
    public Tuple2<String, UniProtKBEntry> call(String line) throws Exception {
        String[] tokens = line.split(",");
        String accession = tokens[0].trim();
        String proteinId = tokens[1];
        String reasonType = tokens[2].trim();

        EntryInactiveReasonBuilder reasonBuilder = new EntryInactiveReasonBuilder();
        reasonBuilder.type(InactiveReasonType.valueOf(reasonType.toUpperCase()));
        if (tokens.length == 4 && !tokens[3].equals("-")) {
            reasonBuilder.mergeDemergeTosAdd(tokens[3]);
        }
        UniProtKBEntry inactiveEntry;
        if (Utils.notNull(proteinId) && !proteinId.trim().isEmpty()) {
            inactiveEntry =
                    new UniProtKBEntryBuilder(accession, proteinId.trim(), reasonBuilder.build())
                            .build();
        } else {
            inactiveEntry = new UniProtKBEntryBuilder(accession, reasonBuilder.build()).build();
        }
        return new Tuple2<>(accession, inactiveEntry);
    }
}
