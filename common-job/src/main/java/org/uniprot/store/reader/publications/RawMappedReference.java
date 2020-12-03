package org.uniprot.store.reader.publications;

import java.util.List;
import java.util.Set;

/**
 * Created 02/12/2020
 *
 * @author Edd
 */
public class RawMappedReference {
    String accession;
    String source;
    String sourceId;
    String pubMedId;
    Set<String> categories;
    String annotation;
}
