package uk.ac.ebi.uniprot.indexer.uniprotkb;

import lombok.Builder;
import lombok.Data;

import java.util.Set;

/**
 * Created 16/04/19
 *
 * @author Edd
 */
@Data
@Builder
public class ErrorEntriesToWriteInfo {
//    private int id;
//    private int itemCount;
//    private boolean toWrite;
    private int failureCount;
    private Set<String> accessions;
}
