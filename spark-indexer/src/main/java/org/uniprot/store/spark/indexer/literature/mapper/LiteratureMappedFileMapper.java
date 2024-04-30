package org.uniprot.store.spark.indexer.literature.mapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.core.literature.LiteratureMappedReference;
import org.uniprot.core.literature.impl.LiteratureMappedReferenceBuilder;

import lombok.extern.slf4j.Slf4j;
import scala.Tuple2;

/**
 * Class Responsible to load PIR mapped file to an JavaPairRDD{key=PubmedId,
 * value=LiteratureMappedReference}
 *
 * @author lgonzales
 * @since 2019-12-02
 */
@Slf4j
public class LiteratureMappedFileMapper
        implements PairFunction<String, String, LiteratureMappedReference> {

    private static final long serialVersionUID = -1866448223077034360L;
    private static final Pattern categoryPattern = Pattern.compile("^(\\[.*\\])(.*)");

    /**
     * @param entryString PIR mapped file line String
     * @return JavaPairRDD{key=PubmedId, value=LiteratureMappedReference}
     */
    @Override
    public Tuple2<String, LiteratureMappedReference> call(String entryString) throws Exception {
        String[] lineFields = entryString.split("\t");
        if (lineFields.length >= 4) {
            List<String> categories = new ArrayList<>();
            String annnotation = "";
            if (lineFields.length >= 5) {
                Matcher matcher = categoryPattern.matcher(lineFields[4]);
                if (matcher.matches()) { // split categories from the rest of the text...
                    String matchedCategories = matcher.group(1);
                    categories.addAll(getCategories(matchedCategories));
                    annnotation = lineFields[4].substring(matchedCategories.length());
                } else {
                    annnotation = lineFields[4];
                }
            }

            LiteratureMappedReference mappedReference =
                    new LiteratureMappedReferenceBuilder()
                            .uniprotAccession(lineFields[0])
                            .source(lineFields[1])
                            .sourceId(lineFields[3])
                            .annotation(annnotation)
                            .sourceCategoriesSet(categories)
                            .build();

            return new Tuple2<>(lineFields[2], mappedReference);
        } else {
            log.warn("Unable to parse correctly line [" + entryString + "]");
            return null;
        }
    }

    private List<String> getCategories(String matchedCategories) {
        String[] categoriesArray = matchedCategories.split("]");
        return Arrays.stream(categoriesArray)
                .map(category -> category.substring(1))
                .collect(Collectors.toList());
    }
}
