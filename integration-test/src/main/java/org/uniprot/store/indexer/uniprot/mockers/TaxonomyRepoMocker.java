package org.uniprot.store.indexer.uniprot.mockers;


import java.io.File;

import org.uniprot.core.cv.taxonomy.FileNodeIterable;
import org.uniprot.core.cv.taxonomy.TaxonomyMapRepo;
import org.uniprot.core.cv.taxonomy.TaxonomyRepo;

public class TaxonomyRepoMocker {

    public static TaxonomyRepo getTaxonomyRepo() {
        String filePath= Thread.currentThread().getContextClassLoader().getResource("taxonomy/taxonomy.dat").getFile();
        File taxonomicFile = new File(filePath);

        FileNodeIterable taxonomicNodeIterable = new FileNodeIterable(taxonomicFile);
        return new TaxonomyMapRepo(taxonomicNodeIterable);
    }

}
