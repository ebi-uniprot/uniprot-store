package org.uniprot.store.spark.indexer.genecentric;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getInputReleaseDirPath;

import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.genecentric.mapper.FastaToCanonicalGeneCentricEntry;
import org.uniprot.store.spark.indexer.genecentric.mapper.FastaToGeneCentricEntry;

import com.typesafe.config.Config;

/**
 * @author lgonzales
 * @since 20/10/2020
 */
public class GeneCentricCanonicalRDDReader extends GeneCentricRDDReader {

    private final JobParameter jobParameter;

    public GeneCentricCanonicalRDDReader(JobParameter jobParameter) {
        super(jobParameter);
        this.jobParameter = jobParameter;
    }

    @Override
    public FastaToGeneCentricEntry getFastaMapper() {
        return new FastaToCanonicalGeneCentricEntry();
    }

    @Override
    public String getFastaFilePath() {
        Config config = jobParameter.getApplicationConfig();
        String releaseInputDir = getInputReleaseDirPath(config, jobParameter.getReleaseName());
        return releaseInputDir + config.getString("genecentric.canonical.fasta.files");
    }
}
