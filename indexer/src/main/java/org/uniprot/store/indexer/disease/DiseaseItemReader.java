package org.uniprot.store.indexer.disease;

import java.io.IOException;
import java.util.Iterator;

import org.springframework.batch.item.ItemReader;
import org.uniprot.core.cv.disease.Disease;
import org.uniprot.core.cv.disease.DiseaseFileReader;

public class DiseaseItemReader implements ItemReader<Disease> {
    private Iterator<Disease> diseaseIterator;

    public DiseaseItemReader(String filePath) throws IOException {
        DiseaseFileReader diseaseFileReader = new DiseaseFileReader();
        this.diseaseIterator = diseaseFileReader.getDiseaseIterator(filePath);
    }

    @Override
    public Disease read() {

        if (this.diseaseIterator.hasNext()) {
            return this.diseaseIterator.next();
        }

        return null;
    }
}
