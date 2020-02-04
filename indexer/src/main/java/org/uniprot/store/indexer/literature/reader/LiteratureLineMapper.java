package org.uniprot.store.indexer.literature.reader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.uniprot.core.DBCrossReference;
import org.uniprot.core.builder.DBCrossReferenceBuilder;
import org.uniprot.core.citation.Author;
import org.uniprot.core.citation.CitationXrefType;
import org.uniprot.core.citation.Literature;
import org.uniprot.core.citation.builder.LiteratureBuilder;
import org.uniprot.core.citation.impl.AuthorImpl;
import org.uniprot.core.citation.impl.PublicationDateImpl;
import org.uniprot.core.literature.LiteratureEntry;
import org.uniprot.core.literature.builder.LiteratureEntryBuilder;
import org.uniprot.core.util.Utils;

/** @author lgonzales */
@Slf4j
public class LiteratureLineMapper extends DefaultLineMapper<LiteratureEntry> {
    private static final String RX_LINE = "RX";
    private static final String RA_LINE = "RA";
    private static final String RT_LINE = "RT";
    private static final String RL_LINE = "RL";
    private static final String RG_LINE = "RG";
    private static final String IP_LINE = "IP";
    private static final String EM_LINE = "EM";
    private static final String NO_ABSTRACT_LINE = "NO ABSTRACT AVAILABLE";
    private static final String COMMENT_LINE = "**";
    private static final String SPLIT_SPACES = "   ";
    private static final String ID_SEPARATOR = ";";
    private static final String LINE_ITEM_SEPARATOR = ",";

    public LiteratureEntry mapLine(String entryString, int lineNumber) throws Exception {
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
                    fileEntry.rgLines.add(tokens[1]);
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

    private LiteratureEntry buildLiteratureEntry(LiteratureFileEntry fileEntry) {
        LiteratureBuilder builder = new LiteratureBuilder();
        builder = parseRXLine(builder, fileEntry.rxLines);
        builder = parseRALine(builder, fileEntry.raLines);
        builder = parseRTLine(builder, fileEntry.rtLines);
        builder = parseRGLine(builder, fileEntry.rgLines);
        builder = parseRLLine(builder, fileEntry.rlLines);
        builder = builder.completeAuthorList(fileEntry.completeAuthorList);
        builder = builder.literatureAbstract(String.join(" ", fileEntry.abstractLines));
        Literature literature = builder.build();
        return new LiteratureEntryBuilder().citation(literature).build();
    }

    private LiteratureBuilder parseRXLine(LiteratureBuilder builder, List<String> rxLines) {
        String rxLine = String.join("", rxLines);
        String[] rxLineArray = rxLine.split(ID_SEPARATOR);
        String pubmedId = rxLineArray[0].substring(rxLineArray[0].indexOf('=') + 1);
        DBCrossReference<CitationXrefType> pubmedXref =
                new DBCrossReferenceBuilder<CitationXrefType>()
                        .databaseType(CitationXrefType.PUBMED)
                        .id(pubmedId)
                        .build();
        builder = builder.addCitationXrefs(pubmedXref);
        if (rxLineArray.length > 1) {
            String doiId = rxLineArray[1].substring(rxLineArray[1].indexOf('=') + 1);
            DBCrossReference<CitationXrefType> doiXref =
                    new DBCrossReferenceBuilder<CitationXrefType>()
                            .databaseType(CitationXrefType.DOI)
                            .id(doiId)
                            .build();
            builder = builder.addCitationXrefs(doiXref);
        }
        return builder;
    }

    private LiteratureBuilder parseRALine(LiteratureBuilder builder, List<String> raLines) {
        if (Utils.notNullNotEmpty(raLines)) {
            String raLine = String.join("", raLines);
            raLine = raLine.substring(0, raLine.length() - 1);
            List<Author> authors =
                    Arrays.stream(raLine.split(LINE_ITEM_SEPARATOR))
                            .filter(author -> !author.isEmpty())
                            .map(String::trim)
                            .map(AuthorImpl::new)
                            .collect(Collectors.toList());
            builder = builder.authorsSet(authors);
        }
        return builder;
    }

    private LiteratureBuilder parseRTLine(LiteratureBuilder builder, List<String> rtLines) {
        if (Utils.notNullNotEmpty(rtLines)) {
            String rtLine = String.join(" ", rtLines);
            builder = builder.title(rtLine.substring(1, rtLine.length() - 2));
        }
        return builder;
    }

    private LiteratureBuilder parseRGLine(LiteratureBuilder builder, List<String> rgLines) {
        List<String> authoringGroup =
                rgLines.stream()
                        .map(ag -> ag.substring(0, ag.length() - 1))
                        .collect(Collectors.toList());
        builder = builder.authoringGroupsSet(authoringGroup);
        return builder;
    }

    //// RL   Journal_abbrev Volume:First_page-Last_page(YYYY).
    private LiteratureBuilder parseRLLine(LiteratureBuilder builder, List<String> rlLines) {
        String rlLine = String.join(" ", rlLines);
        String rlLineJournalAndVolume = rlLine.substring(0, rlLine.indexOf(':'));

        if (rlLineJournalAndVolume.lastIndexOf('.') > 0
                && rlLineJournalAndVolume.lastIndexOf('.') < rlLineJournalAndVolume.length()) {
            String journal = rlLine.substring(0, rlLineJournalAndVolume.lastIndexOf('.') + 1);
            builder = builder.journalName(journal.trim());

            String volume =
                    rlLineJournalAndVolume.substring(rlLineJournalAndVolume.lastIndexOf('.') + 1);
            builder = builder.volume(volume.trim());
        } else {
            String journal = rlLine.substring(0, rlLineJournalAndVolume.lastIndexOf(' '));
            builder = builder.journalName(journal);

            String volume =
                    rlLineJournalAndVolume.substring(rlLineJournalAndVolume.lastIndexOf(' ') + 1);
            builder = builder.volume(volume);
        }

        String rlLinePagesAndYear = rlLine.substring(rlLine.indexOf(':') + 1);
        String[] pages =
                rlLinePagesAndYear.substring(0, rlLinePagesAndYear.indexOf('(')).split("-");
        builder = builder.firstPage(pages[0]);
        if (pages.length > 1) {
            builder = builder.lastPage(pages[1]);
        } else {
            builder = builder.lastPage(pages[0]);
        }

        String publicationYear =
                rlLinePagesAndYear.substring(
                        rlLinePagesAndYear.indexOf('(') + 1, rlLinePagesAndYear.indexOf(')'));
        builder = builder.publicationDate(new PublicationDateImpl(publicationYear));
        return builder;
    }

    private class LiteratureFileEntry {
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
    }
}
