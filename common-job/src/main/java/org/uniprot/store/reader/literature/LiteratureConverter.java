package org.uniprot.store.reader.literature;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.uniprot.core.CrossReference;
import org.uniprot.core.citation.Author;
import org.uniprot.core.citation.CitationDatabase;
import org.uniprot.core.citation.Literature;
import org.uniprot.core.citation.impl.AuthorBuilder;
import org.uniprot.core.citation.impl.LiteratureBuilder;
import org.uniprot.core.citation.impl.PublicationDateBuilder;
import org.uniprot.core.impl.CrossReferenceBuilder;
import org.uniprot.core.util.Utils;
import org.uniprot.cv.FileParseException;

import lombok.extern.slf4j.Slf4j;

/**
 * @author lgonzales
 * @since 24/03/2021
 */
@Slf4j
public class LiteratureConverter {
    private static final String RX_LINE = "RX";
    private static final String RA_LINE = "RA";
    private static final String RT_LINE = "RT";
    private static final String RL_LINE = "RL";
    private static final String RG_LINE = "RG";
    private static final String IP_LINE = "IP";
    private static final String EM_LINE = "EM";
    private static final String NO_ABSTRACT_LINE = "NO ABSTRACT AVAILABLE";
    private static final String COMMENT_LINE = "**";
    private static final String SPLIT_SPACES = " {3}";
    private static final String ID_SEPARATOR = ";";
    private static final String LINE_ITEM_SEPARATOR = ",";

    public Literature convert(String entryString) {
        List<String> entryLines =
                Arrays.stream(entryString.split("\n"))
                        .filter(str -> !str.isEmpty())
                        .collect(Collectors.toList());

        LiteratureFileEntry fileEntry = new LiteratureFileEntry();
        for (String line : entryLines) {
            String[] tokens = line.split(SPLIT_SPACES);
            switch (tokens[0]) {
                case RX_LINE:
                    fileEntry.rxLines.add(tokens[1]);
                    break;
                case RA_LINE:
                    fileEntry.raLines.add(tokens[1]);
                    break;
                case RT_LINE:
                    fileEntry.rtLines.add(tokens[1]);
                    break;
                case RL_LINE:
                    fileEntry.rlLines.add(tokens[1]);
                    break;
                case RG_LINE:
                    if (canAddLine(tokens)) {
                        fileEntry.rgLines.add(tokens[1]);
                    }
                    break;
                case COMMENT_LINE:
                    fileEntry.completeAuthorList = false;
                    break;
                case IP_LINE:
                case EM_LINE:
                case NO_ABSTRACT_LINE:
                    // do nothing for now
                    break;
                default:
                    fileEntry.abstractLines.add(line);
            }
        }
        return buildLiteratureEntry(fileEntry);
    }

    private boolean canAddLine(String[] tokens) {
        return tokens.length >= 2;
    }

    private Literature buildLiteratureEntry(LiteratureFileEntry fileEntry) {
        try {
            LiteratureBuilder builder = new LiteratureBuilder();
            parseRXLine(builder, fileEntry.rxLines);
            parseRALine(builder, fileEntry.raLines);
            parseRTLine(builder, fileEntry.rtLines);
            parseRGLine(builder, fileEntry.rgLines);
            parseRLLine(builder, fileEntry.rlLines);
            builder.completeAuthorList(fileEntry.completeAuthorList);
            builder.literatureAbstract(parseAbstractLines(fileEntry));
            return builder.build();
        } catch (Exception e) {
            throw new FileParseException(
                    "Error Converter LiteratureFileEntry:" + fileEntry.toString());
        }
    }

    private String parseAbstractLines(LiteratureFileEntry fileEntry) {
        return String.join(" ", fileEntry.abstractLines).replace("\r", "").replace("\n", "").trim();
    }

    private void parseRXLine(LiteratureBuilder builder, List<String> rxLines) {
        String rxLine = String.join("", rxLines).replace("\r", "").replace("\n", "").trim();
        String[] rxLineArray = rxLine.split(ID_SEPARATOR);
        String pubmedId = rxLineArray[0].substring(rxLineArray[0].indexOf('=') + 1);
        CrossReference<CitationDatabase> pubmedXref =
                new CrossReferenceBuilder<CitationDatabase>()
                        .database(CitationDatabase.PUBMED)
                        .id(pubmedId)
                        .build();
        builder.citationCrossReferencesAdd(pubmedXref);
        if (rxLineArray.length > 1) {
            String doiId = rxLineArray[1].substring(rxLineArray[1].indexOf('=') + 1);
            CrossReference<CitationDatabase> doiXref =
                    new CrossReferenceBuilder<CitationDatabase>()
                            .database(CitationDatabase.DOI)
                            .id(doiId)
                            .build();
            builder.citationCrossReferencesAdd(doiXref);
        }
    }

    private void parseRALine(LiteratureBuilder builder, List<String> raLines) {
        if (Utils.notNullNotEmpty(raLines)) {
            String raLine = String.join("", raLines);
            raLine = raLine.substring(0, raLine.length() - 1);
            List<Author> authors =
                    Arrays.stream(raLine.split(LINE_ITEM_SEPARATOR))
                            .filter(author -> !author.isEmpty())
                            .map(String::trim)
                            .map(auth -> auth.replace(";", ""))
                            .map(aut -> new AuthorBuilder(aut).build())
                            .collect(Collectors.toList());
            builder.authorsSet(authors);
        }
    }

    private void parseRTLine(LiteratureBuilder builder, List<String> rtLines) {
        if (Utils.notNullNotEmpty(rtLines)) {
            String rtLine = String.join(" ", rtLines).replace("\r", "").replace("\n", "").trim();
            builder.title(rtLine.substring(1, rtLine.length() - 2));
        }
    }

    private void parseRGLine(LiteratureBuilder builder, List<String> rgLines) {
        List<String> authoringGroup =
                rgLines.stream()
                        .map(ag -> ag.substring(0, ag.length() - 1))
                        .map(String::trim)
                        .map(authGroup -> authGroup.replace(";", ""))
                        .collect(Collectors.toList());
        builder.authoringGroupsSet(authoringGroup);
    }

    //// RL   Journal_abbrev Volume:First_page-Last_page(YYYY).
    private void parseRLLine(LiteratureBuilder builder, List<String> rlLines) {
        String rlLine = String.join(" ", rlLines);
        String rlLineJournalAndVolume = rlLine.substring(0, rlLine.indexOf(':'));

        if (rlLineJournalAndVolume.lastIndexOf('.') > 0
                && rlLineJournalAndVolume.lastIndexOf('.') < rlLineJournalAndVolume.length()) {
            String journal = rlLine.substring(0, rlLineJournalAndVolume.lastIndexOf('.') + 1);
            builder.journalName(journal.trim());

            String volume =
                    rlLineJournalAndVolume.substring(rlLineJournalAndVolume.lastIndexOf('.') + 1);
            builder.volume(volume.trim());
        } else {
            String journal = rlLine.substring(0, rlLineJournalAndVolume.lastIndexOf(' '));
            builder.journalName(journal);

            String volume =
                    rlLineJournalAndVolume.substring(rlLineJournalAndVolume.lastIndexOf(' ') + 1);
            builder.volume(volume);
        }

        String rlLinePagesAndYear = rlLine.substring(rlLine.indexOf(':') + 1);
        String[] pages =
                rlLinePagesAndYear.substring(0, rlLinePagesAndYear.indexOf('(')).split("-");
        builder.firstPage(pages[0]);
        if (pages.length > 1) {
            builder.lastPage(pages[1]);
        } else {
            builder.lastPage(pages[0]);
        }

        String publicationYear =
                rlLinePagesAndYear.substring(
                        rlLinePagesAndYear.indexOf('(') + 1, rlLinePagesAndYear.indexOf(')'));
        builder.publicationDate(new PublicationDateBuilder(publicationYear).build());
    }

    private static class LiteratureFileEntry {
        List<String> rxLines;
        List<String> raLines;
        List<String> rlLines;
        List<String> rgLines;
        List<String> rtLines;
        List<String> abstractLines;
        boolean completeAuthorList;

        LiteratureFileEntry() {
            rxLines = new ArrayList<>();
            raLines = new ArrayList<>();
            rlLines = new ArrayList<>();
            rgLines = new ArrayList<>();
            rtLines = new ArrayList<>();
            abstractLines = new ArrayList<>();
            completeAuthorList = true;
        }

        @Override
        public String toString() {
            return "rxLines="
                    + String.join(" ", rxLines)
                    + "\n raLines="
                    + String.join(" ", raLines)
                    + "\n rlLines="
                    + String.join(" ", rlLines)
                    + "\n rgLines="
                    + String.join(" ", rgLines)
                    + "\n rtLines="
                    + String.join(" ", rtLines)
                    + "\n abstractLines="
                    + String.join(" ", abstractLines);
        }
    }
}
