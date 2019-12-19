package indexer.uniprot.mapper;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.store.indexer.uniprot.inactiveentry.InactiveUniProtEntry;

import scala.Serializable;
import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2019-12-02
 */
@Slf4j
public class InactiveFileToInactiveEntry
        implements PairFunction<String, String, InactiveUniProtEntry>, Serializable {

    private static final long serialVersionUID = 8571366803867491177L;

    @Override
    public Tuple2<String, InactiveUniProtEntry> call(String line) throws Exception {
        InactiveUniProtEntry entry = null;
        String[] tokens = line.split(",");
        if (tokens.length == 3) {
            entry = InactiveUniProtEntry.from(tokens[0], tokens[1], tokens[2], "-");
        } else if (tokens.length == 4) {
            entry = InactiveUniProtEntry.from(tokens[0], tokens[1], tokens[2], tokens[3]);
        } else {
            log.info("Unable to parse line: " + line);
        }
        return new Tuple2<>(tokens[0], entry);
    }
}
