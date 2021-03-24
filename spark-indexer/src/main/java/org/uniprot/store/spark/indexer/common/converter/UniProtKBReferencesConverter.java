package org.uniprot.store.spark.indexer.common.converter;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.uniprot.core.flatfile.parser.UniprotKBLineParser;
import org.uniprot.core.flatfile.parser.impl.DefaultUniprotKBLineParserFactory;
import org.uniprot.core.flatfile.parser.impl.entry.EntryObject;
import org.uniprot.core.flatfile.parser.impl.entry.ReferenceObjectConverter;
import org.uniprot.core.flatfile.parser.impl.ra.RaLineObject;
import org.uniprot.core.flatfile.parser.impl.rc.RcLineObject;
import org.uniprot.core.flatfile.parser.impl.rg.RgLineObject;
import org.uniprot.core.flatfile.parser.impl.rl.RlLineObject;
import org.uniprot.core.flatfile.parser.impl.rn.RnLineObject;
import org.uniprot.core.flatfile.parser.impl.rp.RpLineObject;
import org.uniprot.core.flatfile.parser.impl.rt.RtLineObject;
import org.uniprot.core.flatfile.parser.impl.rx.RxLineObject;
import org.uniprot.core.flatfile.writer.LineType;
import org.uniprot.core.uniprotkb.UniProtKBReference;

/**
 * @author lgonzales
 * @since 24/03/2021
 */
public class UniProtKBReferencesConverter {

    private static final Pattern REFERENCE_REGEX = Pattern.compile("^(RN|RA|RC|RG|RL|RP|RT|RX) .*");

    public List<UniProtKBReference> convert(String[] lines) {
        // for each reference in the entry, create a list of lines related to it
        List<List<String>> rawRefs = new ArrayList<>();
        List<String> rawRef = new ArrayList<>();
        for (String line : lines) {
            if (REFERENCE_REGEX.matcher(line).matches()) {
                if (line.startsWith("RN ")) {
                    if (!rawRef.isEmpty()) {
                        rawRefs.add(rawRef);
                    }
                    rawRef = new ArrayList<>();
                }

                rawRef.add(line + "\n");
            }
        }

        rawRefs.add(rawRef);

        return rawRefs.stream()
                .map(this::getSections)
                .map(this::createReference)
                .collect(Collectors.toList());
    }

    private List<LineTypeSection> getSections(List<String> lines) {
        List<LineTypeSection> lineTypeSections = new ArrayList<>();

        boolean isNotLastSection = true;
        int offset = 0;
        while (isNotLastSection) {
            LineTypeSection section = getSectionWithSameLineType(lines, offset);
            lineTypeSections.add(section);
            isNotLastSection = section.isNotLastSection;
            offset = section.lineNumber;
        }

        return lineTypeSections;
    }

    private LineTypeSection getSectionWithSameLineType(List<String> lines, int offset) {
        LineTypeSection lineTypeSection = new LineTypeSection();

        LineType lineType = null;
        StringBuilder sb = new StringBuilder();
        int i = offset;
        for (; i < lines.size(); i++) {
            String line = lines.get(i);

            lineType = LineType.valueOf(lines.get(i).substring(0, 2));
            if (lineTypeSection.type == null) {
                lineTypeSection.type = lineType;
            } else if (lineType != lineTypeSection.type) {
                break;
            }

            sb.append(line);
        }

        lineTypeSection.lineNumber = i;
        lineTypeSection.value = sb.toString();
        if (i == lines.size()) {
            lineTypeSection.isNotLastSection = false;
        }

        return lineTypeSection;
    }

    UniProtKBReference createReference(List<LineTypeSection> sections) {
        final UniprotKBLineParser<RaLineObject> raLineParser =
                new DefaultUniprotKBLineParserFactory().createRaLineParser();
        final UniprotKBLineParser<RcLineObject> rcLineParser =
                new DefaultUniprotKBLineParserFactory().createRcLineParser();
        final UniprotKBLineParser<RgLineObject> rgLineParser =
                new DefaultUniprotKBLineParserFactory().createRgLineParser();
        final UniprotKBLineParser<RlLineObject> rlLineParser =
                new DefaultUniprotKBLineParserFactory().createRlLineParser();
        final UniprotKBLineParser<RnLineObject> rnLineParser =
                new DefaultUniprotKBLineParserFactory().createRnLineParser();
        final UniprotKBLineParser<RpLineObject> rpLineParser =
                new DefaultUniprotKBLineParserFactory().createRpLineParser();
        final UniprotKBLineParser<RtLineObject> rtLineParser =
                new DefaultUniprotKBLineParserFactory().createRtLineParser();
        final UniprotKBLineParser<RxLineObject> rxLineParser =
                new DefaultUniprotKBLineParserFactory().createRxLineParser();

        ReferenceObjectConverter referenceObjectConverter = new ReferenceObjectConverter();
        EntryObject.ReferenceObject referenceObject = new EntryObject.ReferenceObject();

        for (LineTypeSection section : sections) {
            switch (section.type) {
                case RA:
                    referenceObject.ra = raLineParser.parse(section.value);
                    break;
                case RC:
                    referenceObject.rc = rcLineParser.parse(section.value);
                    break;
                case RG:
                    referenceObject.rg = rgLineParser.parse(section.value);
                    break;
                case RL:
                    referenceObject.rl = rlLineParser.parse(section.value);
                    break;
                case RN:
                    referenceObject.rn = rnLineParser.parse(section.value);
                    break;
                case RP:
                    referenceObject.rp = rpLineParser.parse(section.value);
                    break;
                case RT:
                    referenceObject.rt = rtLineParser.parse(section.value);
                    break;
                case RX:
                    referenceObject.rx = rxLineParser.parse(section.value);
                    break;
                default:
                    throw new IllegalStateException(
                            "Unexpected line type encountered: " + section.value);
            }
        }

        return referenceObjectConverter.convert(referenceObject);
    }

    /** Represents all lines of the same type (e.g., RT) of a UniProtKB entry flatfile. */
    static class LineTypeSection {
        int lineNumber = 0;
        LineType type;
        String value;
        boolean isNotLastSection = true;
    }
}
