package org.uniprot.store.indexer.literature.reader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.uniprot.core.CrossReference;
import org.uniprot.core.builder.CrossReferenceBuilder;
import org.uniprot.core.citation.CitationDatabase;
import org.uniprot.core.citation.Literature;
import org.uniprot.core.citation.builder.LiteratureBuilder;
import org.uniprot.core.literature.LiteratureEntry;
import org.uniprot.core.literature.LiteratureMappedReference;
import org.uniprot.core.literature.LiteratureStoreEntry;
import org.uniprot.core.literature.builder.LiteratureEntryBuilder;
import org.uniprot.core.literature.builder.LiteratureMappedReferenceBuilder;
import org.uniprot.core.literature.builder.LiteratureStoreEntryBuilder;

/** @author lgonzales */
@Slf4j
public class LiteratureMappingLineMapper extends DefaultLineMapper<LiteratureStoreEntry> {

    private Pattern p = Pattern.compile("^(\\[.*\\])(.*)");

    public LiteratureStoreEntry mapLine(String entryString, int lineNumber) throws Exception {
        String[] lineFields = entryString.split("\t");
        if (lineFields.length == 4 || lineFields.length == 5) {
            List<String> categories = new ArrayList<>();
            String annnotation = "";
            if (lineFields.length == 5) {
                Matcher matcher = p.matcher(lineFields[4]);
                if (matcher.matches()) { // split categories from the rest of the text...
                    String matchedCategories = matcher.group(1);
                    String[] categoriesArray = matchedCategories.split("]");
                    categories.addAll(
                            Arrays.stream(categoriesArray)
                                    .map(category -> category.substring(1))
                                    .collect(Collectors.toList()));

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

            CrossReference<CitationDatabase> xref =
                    new CrossReferenceBuilder<CitationDatabase>()
                            .databaseType(CitationDatabase.PUBMED)
                            .id(lineFields[2])
                            .build();

            Literature literature = new LiteratureBuilder().citationXrefsAdd(xref).build();

            LiteratureEntry entry = new LiteratureEntryBuilder().citation(literature).build();

            return new LiteratureStoreEntryBuilder()
                    .literatureMappedReferencesAdd(mappedReference)
                    .literatureEntry(entry)
                    .build();
        } else {
            log.warn(
                    "Unable to parse correctly line number ["
                            + lineNumber
                            + "] with value: "
                            + entryString);
            return null;
        }
    }
}
