package org.uniprot.store.spark.indexer.genecentric.mapper;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.spark.api.java.function.Function2;
import org.uniprot.core.genecentric.GeneCentricEntry;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 21/10/2020
 */
public abstract class FastaToGeneCentricEntry
        implements Function2<
                        InputSplit,
                        Iterator<Tuple2<LongWritable, Text>>,
                        Iterator<Tuple2<String, GeneCentricEntry>>>,
        GeneCentricFileNameParser {
    private static final long serialVersionUID = -239002392285087820L;

    @Override
    public Iterator<Tuple2<String, GeneCentricEntry>> call(
            InputSplit inputSplit, Iterator<Tuple2<LongWritable, Text>> entries) throws Exception {
        List<Tuple2<String, GeneCentricEntry>> result = new ArrayList<>();

        final String proteomeId = parseProteomeId((FileSplit) inputSplit);
        entries.forEachRemaining(fastaTuple -> result.add(parseEntry(proteomeId, fastaTuple)));

        return result.iterator();
    }

    abstract Tuple2<String, GeneCentricEntry> parseEntry(
            String proteomeId, Tuple2<LongWritable, Text> fastaTuple);
}
