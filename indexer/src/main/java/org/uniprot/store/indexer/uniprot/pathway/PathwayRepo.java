package org.uniprot.store.indexer.uniprot.pathway;

import org.uniprot.core.cv.pathway.UniPathway;

public interface PathwayRepo {
    UniPathway getFromName(String name);
}
