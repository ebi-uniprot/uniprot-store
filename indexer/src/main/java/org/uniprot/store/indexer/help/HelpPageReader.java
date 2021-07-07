package org.uniprot.store.indexer.help;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import org.uniprot.store.search.document.help.HelpDocument;

/**
 * @author sahmad
 * @created 06/07/2021
 */
@Slf4j
public class HelpPageReader {
    private static final String META_REGION_SEP = "---";
    private static final String CATEGORIES_COLON = "categories:";
    private static final String TITLE_COLON = "title:";

    public HelpDocument read(String fileName) throws IOException {
        log.info("Reading file {}", fileName);
        HelpDocument.HelpDocumentBuilder builder = HelpDocument.builder();
        builder.id(extractId(fileName));
        try (Scanner scanner = new Scanner(new File(fileName), StandardCharsets.UTF_8)) {
            boolean startMetaRegion = false;
            boolean endMetaRegion = false;
            StringBuilder contentBuilder = new StringBuilder();
            while (scanner.hasNext()) {
                String lines = scanner.nextLine();
                if (!endMetaRegion && META_REGION_SEP.equals(lines)) {
                    if (!startMetaRegion) {
                        startMetaRegion = true;
                    } else { // encountered end ---
                        endMetaRegion = true;
                    }
                } else if (endMetaRegion) { // content
                    contentBuilder.append(lines);
                    contentBuilder.append("\n");
                } else if (startMetaRegion) { // in meta block i.e. between --- and ---
                    populateMeta(builder, lines);
                }
            }
            contentBuilder.deleteCharAt(contentBuilder.lastIndexOf("\n"));
            builder.content(contentBuilder.toString());
        }
        return builder.build();
    }

    private String extractId(String filePath) {
        String fileName = filePath.substring(filePath.lastIndexOf("/") + 1);
        return fileName.split(".md")[0]; // get first part from the fileName.md
    }

    private void populateMeta(HelpDocument.HelpDocumentBuilder builder, String line) {
        if (line.startsWith(CATEGORIES_COLON)) {
            List<String> metaValues =
                    Arrays.stream(line.split(CATEGORIES_COLON)[1].split(","))
                            .map(String::strip)
                            .map(cat -> cat.replace("_", " "))
                            .collect(Collectors.toList());
            builder.categories(metaValues);
        }

        if (line.startsWith(TITLE_COLON)) {
            builder.title(line.split(TITLE_COLON)[1].strip());
        }
    }
}
