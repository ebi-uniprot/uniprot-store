package org.uniprot.store.spark.indexer.genecentric;

import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.api.java.JavaNewHadoopRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.uniprot.core.genecentric.GeneCentricEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.reader.PairRDDReader;
import org.uniprot.store.spark.indexer.genecentric.mapper.FastaToGeneCentricEntry;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 09/11/2020
 */
public abstract class GeneCentricRDDReader implements PairRDDReader<String, GeneCentricEntry> {

    private final JobParameter jobParameter;

    private static final String SPLITTER = "\n>";

    public GeneCentricRDDReader(JobParameter jobParameter) {
        this.jobParameter = jobParameter;
    }

    /**
     * @return an JavaPairRDD with <accession, GeneCentricEntry>
     */
    @Override
    public JavaPairRDD<String, GeneCentricEntry> load() {
        return loadWithMapper(getFastaMapper());
    }

    protected <T> JavaPairRDD<String, T> loadWithMapper(
            Function2<InputSplit, Iterator<Tuple2<LongWritable, Text>>, Iterator<Tuple2<String, T>>>
                    mapper) {
        JavaSparkContext jsc = jobParameter.getSparkContext();
        jsc.hadoopConfiguration().set("textinputformat.record.delimiter", SPLITTER);
        String filePath = getFastaFilePath();

        JavaPairRDD<LongWritable, Text> javaPairRDD =
                jsc.newAPIHadoopFile(
                        filePath,
                        TextInputFormat.class,
                        LongWritable.class,
                        Text.class,
                        jsc.hadoopConfiguration());
        return ((JavaNewHadoopRDD<LongWritable, Text>) javaPairRDD)
                .mapPartitionsWithInputSplit(mapper, true)
                .mapToPair(tuple -> tuple);
    }

    abstract FastaToGeneCentricEntry getFastaMapper();

    abstract String getFastaFilePath();
}
