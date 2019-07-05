package uk.ac.ebi.uniprot.indexer.literature.reader;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import uk.ac.ebi.uniprot.domain.literature.LiteratureEntry;
import uk.ac.ebi.uniprot.domain.literature.LiteratureMappedReference;
import uk.ac.ebi.uniprot.domain.literature.builder.LiteratureEntryBuilder;
import uk.ac.ebi.uniprot.domain.literature.builder.LiteratureMappedReferenceBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author lgonzales
 */
@Slf4j
public class LiteratureMappingLineMapper extends DefaultLineMapper<LiteratureEntry> {

    private Pattern p = Pattern.compile("^(\\[.*\\])(.*)");


    public LiteratureEntry mapLine(String entryString, int lineNumber) throws Exception {
        String[] lineFields = entryString.split("\t");
        if (lineFields.length == 4 || lineFields.length == 5) {
            List<String> categories = new ArrayList<>();
            String annnotation = "";
            if (lineFields.length == 5) {
                Matcher matcher = p.matcher(lineFields[4]);
                if (matcher.matches()) { //split categories from the rest of the text...
                    String matchedCategories = matcher.group(1);
                    String[] categoriesArray = matchedCategories.split("]");
                    categories.addAll(Arrays.stream(categoriesArray)
                            .map(category -> category.substring(1))
                            .collect(Collectors.toList()));

                    annnotation = lineFields[4].substring(matchedCategories.length());
                } else {
                    annnotation = lineFields[4];
                }
            }
            LiteratureEntryBuilder entryBuilder = new LiteratureEntryBuilder();
            LiteratureMappedReference mappedReference = new LiteratureMappedReferenceBuilder()
                    .uniprotAccession(lineFields[0])
                    .source(lineFields[1])
                    .sourceId(lineFields[3])
                    .annotation(annnotation)
                    .sourceCategory(categories)
                    .build();

            return entryBuilder.pubmedId(lineFields[2]).addLiteratureMappedReference(mappedReference).build();
        } else {
            log.warn("Unable to parse correctly line number [" + lineNumber + "] with value: " + entryString);
            return null;
        }
    }
}
