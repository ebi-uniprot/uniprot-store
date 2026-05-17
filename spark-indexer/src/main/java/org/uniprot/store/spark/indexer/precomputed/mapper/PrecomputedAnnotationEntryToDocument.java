package org.uniprot.store.spark.indexer.precomputed.mapper;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;

import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.store.search.document.precomputed.PrecomputedAnnotationDocument;
import org.uniprot.store.spark.indexer.common.exception.SparkIndexException;

import scala.Tuple2;

public class PrecomputedAnnotationEntryToDocument
        implements PairFunction<String, String, PrecomputedAnnotationDocument> {

    private static final long serialVersionUID = -6570815106805565054L;
    private static final Pattern ACCESSION_PATTERN = Pattern.compile("(?m)^AC\\s+([^;\\s]+)\\s*;");

    @Override
    public Tuple2<String, PrecomputedAnnotationDocument> call(String entry) {
        String accession = getAccession(entry);
        return new Tuple2<>(
                getTaxonomy(accession),
                PrecomputedAnnotationDocument.builder().accession(accession).build());
    }

    String getAccession(String entry) {
        Matcher matcher = ACCESSION_PATTERN.matcher(entry);
        if (!matcher.find()) {
            throw new SparkIndexException("Unable to find AC line in precomputed annotation entry");
        }
        return matcher.group(1);
    }

    String getTaxonomy(String accession) {
        int separatorIndex = accession.lastIndexOf('-');
        if (separatorIndex < 0 || separatorIndex == accession.length() - 1) {
            throw new SparkIndexException(
                    "Unable to extract taxonomy from precomputed annotation accession: "
                            + accession);
        }
        return accession.substring(separatorIndex + 1);
    }

    static PrecomputedAnnotationDocument withProteomes(
            PrecomputedAnnotationDocument document, Iterable<String> proteomeIds) {
        List<String> sortedProteomeIds =
                StreamSupport.stream(proteomeIds.spliterator(), false).distinct().sorted().toList();
        if (sortedProteomeIds.isEmpty()) {
            throw new SparkIndexException(
                    "Unable to find proteome ids for precomputed annotation accession: "
                            + document.getAccession());
        }
        return document.toBuilder().proteome(sortedProteomeIds).build();
    }
}
