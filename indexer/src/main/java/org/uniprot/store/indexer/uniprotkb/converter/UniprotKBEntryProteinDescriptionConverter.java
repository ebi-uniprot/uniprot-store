package org.uniprot.store.indexer.uniprotkb.converter;

import static org.uniprot.store.indexer.common.utils.UniProtAARuleUtils.extractProteinDescriptionEcs;
import static org.uniprot.store.indexer.uniprotkb.converter.UniProtEntryConverterUtil.*;

import java.util.List;
import java.util.Map;

import org.uniprot.core.uniprotkb.description.*;
import org.uniprot.cv.ec.ECRepo;
import org.uniprot.store.search.document.suggest.SuggestDictionary;
import org.uniprot.store.search.document.suggest.SuggestDocument;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

/**
 * @author lgonzales
 * @since 2019-09-05
 */
class UniprotKBEntryProteinDescriptionConverter {

    private final ECRepo ecRepo;
    private final Map<String, SuggestDocument> suggestions;

    UniprotKBEntryProteinDescriptionConverter(
            ECRepo ecRepo, Map<String, SuggestDocument> suggestions) {
        this.ecRepo = ecRepo;
        this.suggestions = suggestions;
    }

    void convertProteinDescription(
            ProteinDescription proteinDescription, UniProtDocument document) {
        if (proteinDescription != null) {
            List<String> names = extractProteinDescriptionValues(proteinDescription);
            document.proteinNames.addAll(names);
            document.proteinsNamesSort = truncatedSortValue(String.join(" ", names));

            convertECNumbers(proteinDescription, document);
            convertFragmentNPrecursor(proteinDescription, document);
        }
    }

    private void convertECNumbers(ProteinDescription proteinDescription, UniProtDocument document) {
        List<String> ecNumbers = extractProteinDescriptionEcs(proteinDescription);
        document.ecNumbers = ecNumbers;
        document.ecNumbersExact = document.ecNumbers;

        for (String ecNumber : ecNumbers) {
            ecRepo.getEC(ecNumber)
                    .ifPresent(
                            ec ->
                                    suggestions.putIfAbsent(
                                            createSuggestionMapKey(SuggestDictionary.EC, ecNumber),
                                            SuggestDocument.builder()
                                                    .id(ecNumber)
                                                    .value(ec.getLabel())
                                                    .dictionary(SuggestDictionary.EC.name())
                                                    .build()));
        }
    }

    private void convertFragmentNPrecursor(
            ProteinDescription proteinDescription, UniProtDocument document) {
        boolean isFragment = false;
        boolean isPrecursor = false;
        if (proteinDescription.hasFlag()) {
            Flag flag = proteinDescription.getFlag();
            switch (flag.getType()) {
                case FRAGMENT:
                case FRAGMENTS:
                    isFragment = true;
                    break;
                case PRECURSOR:
                    isPrecursor = true;
                    break;
                case FRAGMENT_PRECURSOR:
                case FRAGMENTS_PRECURSOR:
                    isPrecursor = true;
                    isFragment = true;
                    break;
            }
        }
        document.fragment = isFragment;
        document.precursor = isPrecursor;
    }
}
