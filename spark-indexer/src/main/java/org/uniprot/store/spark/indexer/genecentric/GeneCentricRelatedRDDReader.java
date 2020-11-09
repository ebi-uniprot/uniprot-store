package org.uniprot.store.spark.indexer.genecentric;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getInputReleaseDirPath;

import java.util.ResourceBundle;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.api.java.JavaNewHadoopRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.core.genecentric.GeneCentricEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.reader.PairRDDReader;
import org.uniprot.store.spark.indexer.genecentric.mapper.FastaToRelatedGeneCentricEntry;

/**
 * @author lgonzales
 * @since 20/10/2020
 */
public class GeneCentricRelatedRDDReader implements PairRDDReader<String, GeneCentricEntry> {

    private final JobParameter jobParameter;

    public GeneCentricRelatedRDDReader(JobParameter jobParameter) {
        this.jobParameter = jobParameter;
    }

    private static final String SPLITTER = "\n>";

    /** @return an JavaPairRDD with <accession, GeneCentricEntry> */
    @Override
    public JavaPairRDD<String, GeneCentricEntry> load() {
        ResourceBundle config = jobParameter.getApplicationConfig();
        JavaSparkContext jsc = jobParameter.getSparkContext();
        String releaseInputDir = getInputReleaseDirPath(config, jobParameter.getReleaseName());
        String filePath = releaseInputDir + config.getString("genecentric.related.fasta.files");
        jsc.hadoopConfiguration().set("textinputformat.record.delimiter", SPLITTER);

        JavaPairRDD<LongWritable, Text> javaPairRDD =
                jsc.newAPIHadoopFile(
                        filePath,
                        TextInputFormat.class,
                        LongWritable.class,
                        Text.class,
                        jsc.hadoopConfiguration());
        JavaNewHadoopRDD<LongWritable, Text> hadoopRDD = (JavaNewHadoopRDD) javaPairRDD;
        return hadoopRDD
                .mapPartitionsWithInputSplit(new FastaToRelatedGeneCentricEntry(), true)
                .mapToPair(tuple -> tuple);
    }
}
