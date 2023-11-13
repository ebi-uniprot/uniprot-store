package org.uniprot.store.spark.indexer.genecentric.mapper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.jupiter.api.Test;

import scala.Tuple2;

import com.google.common.collect.Lists;

class FastaToProteomeGeneCountTest {
    private final FastaToProteomeGeneCount fastaToProteomeGeneCount =
            new FastaToProteomeGeneCount();
    private final InputSplit fileSplit =
            new FileSplit(new Path("UP000000554_64091_additional.fasta"), 0, 0, null);

    @Test
    void call() throws Exception {
        Iterator<Tuple2<LongWritable, Text>> entries =
                List.of(
                                new Tuple2<>(new LongWritable(), new Text()),
                                new Tuple2<>(new LongWritable(), new Text()))
                        .iterator();

        Iterator<Tuple2<String, Integer>> result =
                fastaToProteomeGeneCount.call(fileSplit, entries);

        assertResult(result, 2);
    }

    private static void assertResult(Iterator<Tuple2<String, Integer>> result, int count) {
        assertThat(Lists.newArrayList(result), contains(new Tuple2<>("UP000000554", count)));
    }

    @Test
    void call_whenSplitsAreEmpty() throws Exception {
        Iterator<Tuple2<LongWritable, Text>> entries = Collections.emptyIterator();

        Iterator<Tuple2<String, Integer>> result =
                fastaToProteomeGeneCount.call(fileSplit, entries);

        assertResult(result, 0);
    }
}
