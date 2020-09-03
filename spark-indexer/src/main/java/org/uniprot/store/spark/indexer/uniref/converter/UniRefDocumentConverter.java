package org.uniprot.store.spark.indexer.uniref.converter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.uniprot.core.uniref.UniRefEntry;
import org.uniprot.core.uniref.UniRefMember;
import org.uniprot.core.uniref.UniRefMemberIdType;
import org.uniprot.core.util.Utils;
import org.uniprot.store.indexer.util.DateUtils;
import org.uniprot.store.job.common.converter.DocumentConverter;
import org.uniprot.store.search.document.uniref.UniRefDocument;

/**
 * This class convert an UniRefEntry to UniRefDocument
 *
 * @author lgonzales
 * @since 2020-02-07
 */
@Slf4j
public class UniRefDocumentConverter
        implements DocumentConverter<UniRefEntry, UniRefDocument>, Serializable {

    private static final long serialVersionUID = -1071849666501652399L;

    @Override
    public UniRefDocument convert(UniRefEntry entry) {
        return UniRefDocument.builder()
                .id(entry.getId().getValue())
                .identity(entry.getEntryType().getIdentity())
                .name(entry.getName())
                .count(entry.getMembers().size() + 1)
                .length(entry.getRepresentativeMember().getSequence().getLength())
                .created(DateUtils.convertLocalDateToDate(entry.getUpdated()))
                .uniprotIds(getUniProtIds(entry))
                .upis(getUniParcIds(entry))
                .organismSort(getOrganismNameForSort(entry))
                .taxLineageId((int) entry.getRepresentativeMember().getOrganismTaxId())
                .organismTaxon(entry.getRepresentativeMember().getOrganismName())
                .build();
    }

    private String getOrganismNameForSort(UniRefEntry entry) {
        List<String> result = new ArrayList<>();
        result.add(entry.getRepresentativeMember().getOrganismName());
        entry.getMembers().stream()
                .map(UniRefMember::getOrganismName)
                .distinct()
                .limit(5)
                .forEach(result::add);
        return String.join(" ", result);
    }

    private List<String> getUniParcIds(UniRefEntry entry) {
        List<String> result = getUniParcIds(entry.getRepresentativeMember());
        entry.getMembers().forEach(val -> result.addAll(getUniParcIds(val)));

        return result;
    }

    private List<String> getUniParcIds(UniRefMember member) {
        List<String> result = new ArrayList<>();
        if (member.getMemberIdType() == UniRefMemberIdType.UNIPARC) {
            result.add(member.getMemberId());
        }
        if (Utils.notNull(member.getUniParcId())) {
            result.add(member.getUniParcId().getValue());
        }
        return result;
    }

    private List<String> getUniProtIds(UniRefEntry entry) {
        List<String> result = getUniProtIds(entry.getRepresentativeMember());
        entry.getMembers().forEach(val -> result.addAll(getUniProtIds(val)));

        return result;
    }

    private List<String> getUniProtIds(UniRefMember member) {
        List<String> result = new ArrayList<>();
        if (member.getMemberIdType() != UniRefMemberIdType.UNIPARC) {
            result.add(member.getMemberId());
        }
        member.getUniProtAccessions().forEach(val -> result.add(val.getValue()));

        return result;
    }
}
