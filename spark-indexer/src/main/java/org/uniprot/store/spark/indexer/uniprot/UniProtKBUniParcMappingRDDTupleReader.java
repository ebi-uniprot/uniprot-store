package org.uniprot.store.spark.indexer.uniprot;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getInputReleaseDirPath;

import java.io.Serial;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.exception.SparkIndexException;
import org.uniprot.store.spark.indexer.common.reader.PairRDDReader;
import org.uniprot.store.spark.indexer.uniprot.mapper.UniProtKBUniParcMapper;

import com.typesafe.config.Config;

public class UniProtKBUniParcMappingRDDTupleReader implements PairRDDReader<String, String> {

    private final JobParameter jobParameter;
    private final Boolean active;

    public UniProtKBUniParcMappingRDDTupleReader(JobParameter jobParameter) {
        this.jobParameter = jobParameter;
        this.active = null;
    }

    public UniProtKBUniParcMappingRDDTupleReader(JobParameter jobParameter, boolean active) {
        this.jobParameter = jobParameter;
        this.active = active;
    }

    @Override
    public JavaPairRDD<String, String> load() {
        Config config = jobParameter.getApplicationConfig();
        JavaSparkContext jsc = jobParameter.getSparkContext();

        SparkSession spark = SparkSession.builder().sparkContext(jsc.sc()).getOrCreate();
        String releaseInputDir = getInputReleaseDirPath(config, jobParameter.getReleaseName());
        String filePath = releaseInputDir + config.getString("uniprot.uniparc.map.file.path");
        return spark.read()
                .textFile(filePath)
                .toJavaRDD()
                .filter(new StatusFilter(active))
                .mapToPair(new UniProtKBUniParcMapper());
    }

    private static class StatusFilter implements Function<String, Boolean> {
        @Serial private static final long serialVersionUID = -6243565643037921590L;
        private final Boolean active;

        public StatusFilter(Boolean active) {
            this.active = active;
        }

        @Override
        public Boolean call(String line) {
            if (line == null || line.split("\s").length != 3) {
                throw new SparkIndexException(
                        "Unable to parse UniProtKBUniParcMapping line: " + line);
            }
            String[] tokens = line.split("\s");
            boolean lineStatus = tokens[2].strip().equalsIgnoreCase("Y");
            return active == null || active == lineStatus;
        }
    }
}
