package org.uniprot.store.indexer.help;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

import org.commonmark.node.Node;
import org.commonmark.parser.Parser;
import org.commonmark.renderer.html.HtmlRenderer;
import org.jsoup.Jsoup;
import org.uniprot.store.search.document.help.HelpDocument;

import lombok.extern.slf4j.Slf4j;

/**
 * @author sahmad
 * @created 06/07/2021
 */
@Slf4j
public class HelpPageReader {
    protected static final String CATEGORIES_COLON = "categories:";
    protected static final String TITLE_COLON = "title:";
    protected static final String TYPE_COLON = "type:";
    protected static final String DATE_COLON = "date:";
    private static final String META_REGION_SEP = "---";

    public HelpDocument read(String fileName) throws IOException {
        log.info("Reading file {}", fileName);
        File helpFile = new File(fileName);
        HelpDocument.HelpDocumentBuilder builder = HelpDocument.builder();
        builder.id(extractId(fileName));
        builder.lastModified(new Date(helpFile.lastModified()));
        try (Scanner scanner = new Scanner(helpFile, StandardCharsets.UTF_8)) {
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
            String content = contentBuilder.toString();
            builder.contentOriginal(content);
            builder.content(getCleanContent(content));
        }
        return builder.build();
    }

    protected void populateMeta(HelpDocument.HelpDocumentBuilder builder, String line) {
        String[] splitCategories = line.split(CATEGORIES_COLON);
        if (splitCategories.length == 2) {
            List<String> metaValues =
                    Arrays.stream(splitCategories[1].split(","))
                            .map(String::strip)
                            .map(cat -> cat.replace("_", " "))
                            .collect(Collectors.toList());
            builder.categories(metaValues);
        } else {
            log.warn("No categories set for Help document ID: " + builder);
        }

        String[] splitTitle = line.split(TITLE_COLON);
        if (splitTitle.length == 2) {
            builder.title(splitTitle[1].strip());
        } else {
            log.warn("No title set for Help document ID: " + builder);
        }

        populateType(builder, line);
        populateDate(builder, line);
    }

    private void populateDate(HelpDocument.HelpDocumentBuilder builder, String line) {
        String[] dateType = line.split(DATE_COLON);
        if (dateType.length == 2) {
            try {
                LocalDate localDate = LocalDate.parse(dateType[1].strip());
                Date date = Date.from(localDate.atStartOfDay(ZoneId.systemDefault()).toInstant());

                builder.releaseDate(date);
            } catch (Exception e) {
                log.warn("Failed to parse release date: " + dateType[1]);
            }
        }
    }

    private void populateType(HelpDocument.HelpDocumentBuilder builder, String line) {
        String[] splitType = line.split(TYPE_COLON);
        if (splitType.length == 2) {
            builder.type(splitType[1].strip());
        }
    }

    private String extractId(String filePath) {
        String fileName = filePath.substring(filePath.lastIndexOf("/") + 1);
        return fileName.split(".md")[0]; // get first part from the fileName.md
    }

    private String getCleanContent(String content) {
        Parser parser = Parser.builder().build();
        Node document = parser.parse(content);
        HtmlRenderer renderer = HtmlRenderer.builder().build();
        String htmlContent = renderer.render(document);

        return Jsoup.parse(htmlContent).text();
    }
}
