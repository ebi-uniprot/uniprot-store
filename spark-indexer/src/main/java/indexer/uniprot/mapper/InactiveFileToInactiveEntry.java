package indexer.uniprot.mapper;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.core.uniprot.InactiveReasonType;
import org.uniprot.core.uniprot.UniProtEntry;
import org.uniprot.core.uniprot.builder.EntryInactiveReasonBuilder;
import org.uniprot.core.uniprot.builder.UniProtEntryBuilder;
import org.uniprot.core.util.Utils;

import scala.Serializable;
import scala.Tuple2;

/**
 * This class map a csv string line of an Inactive Entry To a Tuple2{key=accession, value={@link
 * UniProtEntry}}
 *
 * @author lgonzales
 * @since 2019-12-02
 */
@Slf4j
public class InactiveFileToInactiveEntry
        implements PairFunction<String, String, UniProtEntry>, Serializable {

    private static final long serialVersionUID = 8571366803867491177L;

    /**
     * @param line csv file line of an Inactive Entry in String format
     * @return Tuple2{key=accession, value={@link UniProtEntry}}
     */
    @Override
    public Tuple2<String, UniProtEntry> call(String line) throws Exception {
        String[] tokens = line.split(",");
        String accession = tokens[0];
        String proteinId = tokens[1];
        String reasonType = tokens[2];

        EntryInactiveReasonBuilder reasonBuilder = new EntryInactiveReasonBuilder();
        reasonBuilder.type(InactiveReasonType.valueOf(reasonType.toUpperCase()));
        if (tokens.length == 4 && !tokens[3].equals("-")) {
            reasonBuilder.addMergeDemergeTo(tokens[3]);
        }
        UniProtEntry inactiveEntry;
        if (Utils.notNullOrEmpty(proteinId)) {
            inactiveEntry =
                    new UniProtEntryBuilder(accession, proteinId, reasonBuilder.build()).build();
        } else {
            inactiveEntry = new UniProtEntryBuilder(accession, reasonBuilder.build()).build();
        }
        return new Tuple2<>(accession, inactiveEntry);
    }
}
