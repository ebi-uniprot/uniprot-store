package org.uniprot.store.indexer.help;

import org.uniprot.store.search.document.help.HelpDocument;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Scanner;

/**
 * @author sahmad
 * @created 06/07/2021
 */
public class HelpPageReader {
    private static final String META_REGION_SEP = "---";

    public HelpDocument read(String fileName) throws IOException {
        HelpDocument.HelpDocumentBuilder builder = HelpDocument.builder();
        try (Scanner scanner = new Scanner(new File(fileName), StandardCharsets.UTF_8)) {
            boolean startMetaRegion = false;
            boolean endMetaRegion = false;
            StringBuilder description = new StringBuilder();
            while (scanner.hasNext()) {
                String lines = scanner.next();
                scanner.useDelimiter("\\Z");
                description.append(lines);// enable below code once we have prepared proper input file
//                if (!endMetaRegion && META_REGION_SEP.equals(lines)) {
//                    if (!startMetaRegion) {
//                        startMetaRegion = true;
//                    } else {
//                        endMetaRegion = true;
//                    }
//                } else if (endMetaRegion) { // content
//                    description.append(lines);
//                } else { // in meta block
//                    String[] metaTokens = lines.split(":");
//                    if (metaTokens.length != 2) {
//                        throw new IllegalArgumentException("Invalid meta in file " + fileName);
//                    }
//                    populateMeta(builder, metaTokens);
//                }
            }

            builder.description(description.toString());
        }
        return builder.build();
    }

    private void populateMeta(HelpDocument.HelpDocumentBuilder builder, String[] metaTokens) {
        String metaName = metaTokens[0];
        List<String> metaValues = List.of(metaTokens[1].split(","));
        switch (metaName){
            case "categories":
                builder.categories(metaValues);
                break;
            case "keywords":
                builder.keywords(metaValues);
                break;
            case "section":
                builder.sections(metaValues);
                break;
            case "title":
                builder.title(metaValues.get(0));
                break;
            default:
                throw new IllegalArgumentException("Unknown meta name " + metaName);
        }
    }
}
