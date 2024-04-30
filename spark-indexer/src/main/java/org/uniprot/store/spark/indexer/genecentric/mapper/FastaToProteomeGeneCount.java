package org.uniprot.store.spark.indexer.genecentric.mapper;

import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.spark.api.java.function.Function2;

import com.google.common.collect.Iterators;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 21/10/2020
 */
public class FastaToProteomeGeneCount
        implements Function2<
                        InputSplit,
                        Iterator<Tuple2<LongWritable, Text>>,
                        Iterator<Tuple2<String, Integer>>>,
                GeneCentricFileNameParser {

    private static final long serialVersionUID = -3930874101012298316L;

    @Override
    public Iterator<Tuple2<String, Integer>> call(
            InputSplit inputSplit, Iterator<Tuple2<LongWritable, Text>> entries) throws Exception {
        final String proteomeId = parseProteomeId((FileSplit) inputSplit);
        return List.of(new Tuple2<>(proteomeId, Iterators.size(entries))).iterator();
    }
}
