package org.uniprot.store.indexer.uniprot.pathway;

import org.uniprot.cv.pathway.UniPathway;

public interface PathwayRepo {
    UniPathway getFromName(String name);
}
