package org.uniprot.store.spark.indexer.literature;

import java.math.BigInteger;
import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.uniprot.core.uniprotkb.UniProtKBReference;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.writer.DocumentsToHDFSWriter;
import org.uniprot.store.spark.indexer.literature.mapper.LiteratureUniProtKBReferencesMapper;
import org.uniprot.store.spark.indexer.uniprot.UniProtKBRDDTupleReader;

/**
 * @author lgonzales
 * @since 24/03/2021
 */
public class LiteratureDocumentsToHDFSWriter implements DocumentsToHDFSWriter {

    private final JobParameter parameter;
    private final ResourceBundle config;
    private final String releaseName;

    public LiteratureDocumentsToHDFSWriter(JobParameter parameter) {
        this.parameter = parameter;
        this.config = parameter.getApplicationConfig();
        this.releaseName = parameter.getReleaseName();
    }

    @Override
    public void writeIndexDocumentsToHDFS() {
        LiteratureRDDTupleReader literatureReader = new LiteratureRDDTupleReader(parameter);

        // JavaPairRDD<String, Literature> literature = literatureReader.load();
    }

    private JavaPairRDD<String, UniProtKBReference> loadUniProtKBMappedRefs() {
        UniProtKBRDDTupleReader uniProtKBReader =
                new UniProtKBRDDTupleReader(this.parameter, false);
        JavaRDD<String> uniProtKBEntryStringsRDD = uniProtKBReader.loadFlatFileToRDD();

        return uniProtKBEntryStringsRDD.flatMapToPair(new LiteratureUniProtKBReferencesMapper());
    }

    public static void main(String[] args) {
        String input = "BFCBC45A16B8E321";
        System.out.println(input);
        BigInteger num = new BigInteger(input, 16); // Store as Hexa
        System.out.println(num.toString(32));
    }
}
