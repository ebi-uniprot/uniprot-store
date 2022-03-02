package org.uniprot.store.indexer.uniref.mockers;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import org.uniprot.core.Sequence;
import org.uniprot.core.cv.go.GoAspect;
import org.uniprot.core.cv.go.impl.GeneOntologyEntryBuilder;
import org.uniprot.core.impl.SequenceBuilder;
import org.uniprot.core.uniparc.impl.UniParcIdBuilder;
import org.uniprot.core.uniprotkb.impl.UniProtKBAccessionBuilder;
import org.uniprot.core.uniprotkb.taxonomy.Organism;
import org.uniprot.core.uniprotkb.taxonomy.impl.OrganismBuilder;
import org.uniprot.core.uniref.*;
import org.uniprot.core.uniref.impl.*;

/**
 * @author jluo
 * @date: 23 Aug 2019
 */
public class UniRefEntryMocker {

    private UniRefEntryMocker() {}

    public static final String ID_PREF_50 = "UniRef50_P039";
    public static final String ID_PREF_90 = "UniRef90_P039";
    public static final String ID_PREF_100 = "UniRef100_P039";
    public static final String NAME_PREF = "Cluster: MoeK5 ";
    public static final String ACC_PREF = "P123";
    public static final String ACC_2_PREF = "P321";
    public static final String UPI_PREF = "UPI0000083A";

    public static UniRefEntry createEntry(int i, int numberOfMembers, UniRefType type) {
        UniRefEntryBuilder builder = UniRefEntryBuilder.from(createEntry(i, type));
        for (int j = 2; j < numberOfMembers; j++) {
            builder.membersAdd(createMember(j));
        }
        builder.memberCount(numberOfMembers); // member plus representative
        return builder.build();
    }

    public static List<RepresentativeMember> createEntryMembers(UniRefEntry entry) {
        List<RepresentativeMember> members = new ArrayList<>();
        members.add(entry.getRepresentativeMember());
        entry.getMembers()
                .forEach(member -> members.add(RepresentativeMemberBuilder.from(member).build()));
        return members;
    }

    public static UniRefEntry createEntry(int i, UniRefType type) {
        String idRef = getIdRef(type);

        UniRefEntryId entryId = new UniRefEntryIdBuilder(getName(idRef, i)).build();

        Organism commonOrganism =
                new OrganismBuilder().taxonId(9606L).scientificName("Homo sapiens").build();

        return new UniRefEntryBuilder()
                .id(entryId)
                .name(getName(NAME_PREF, i))
                .updated(LocalDate.of(2019, 8, 27))
                .entryType(type)
                .commonTaxon(commonOrganism)
                .seedId(getName(ACC_2_PREF, i))
                .representativeMember(createReprestativeMember(i))
                .membersAdd(createMember(i))
                .goTermsAdd(
                        new GeneOntologyEntryBuilder()
                                .aspect(GoAspect.COMPONENT)
                                .id("GO:0044444")
                                .build())
                .goTermsAdd(
                        new GeneOntologyEntryBuilder()
                                .aspect(GoAspect.FUNCTION)
                                .id("GO:0044459")
                                .build())
                .goTermsAdd(
                        new GeneOntologyEntryBuilder()
                                .aspect(GoAspect.PROCESS)
                                .id("GO:0032459")
                                .build())
                .memberCount(2)
                .build();
    }

    private static String getIdRef(UniRefType type) {
        switch (type) {
            case UniRef50:
                return ID_PREF_50;
            case UniRef90:
                return ID_PREF_90;
            default:
                return ID_PREF_100;
        }
    }

    public static UniRefMember createMember(int i) {
        String memberId = getName(ACC_2_PREF, i) + "_HUMAN";
        int length = 312;
        String pName = "some protein name";
        String upi = getName(UPI_PREF, i);

        UniRefMemberIdType type = UniRefMemberIdType.UNIPROTKB;
        return new UniRefMemberBuilder()
                .memberIdType(type)
                .memberId(memberId)
                .organismName("Homo sapiens " + i)
                .organismTaxId(9606L + i)
                .sequenceLength(length)
                .proteinName(pName)
                .uniparcId(new UniParcIdBuilder(upi).build())
                .accessionsAdd(new UniProtKBAccessionBuilder(getName(ACC_2_PREF, i)).build())
                .uniref100Id(new UniRefEntryIdBuilder("UniRef100_P03923").build())
                .uniref90Id(new UniRefEntryIdBuilder("UniRef90_P03943").build())
                .uniref50Id(new UniRefEntryIdBuilder("UniRef50_P03973").build())
                .build();
    }

    public static String getName(String prefix, int i) {
        if (i < 10) {
            return prefix + "0" + i;
        } else return prefix + i;
    }

    public static RepresentativeMember createReprestativeMember(int i) {
        String seq = "MVSWGRFICLVVVTMATLSLARPSFSLVEDDFSAGSADFAFWERDGDSDGFDSHSDJHETRHJREH";
        Sequence sequence = new SequenceBuilder(seq).build();
        String memberId = getName(ACC_PREF, i) + "_HUMAN";
        int length = 312;
        String pName = "some protein name";
        String upi = getName(UPI_PREF, i);

        UniRefMemberIdType type = UniRefMemberIdType.UNIPROTKB_TREMBL;

        return new RepresentativeMemberBuilder()
                .memberIdType(type)
                .memberId(memberId)
                .organismName("Homo sapiens (human)")
                .organismTaxId(9600)
                .sequenceLength(length)
                .proteinName(pName)
                .uniparcId(new UniParcIdBuilder(upi).build())
                .accessionsAdd(new UniProtKBAccessionBuilder(getName(ACC_PREF, i)).build())
                .uniref100Id(new UniRefEntryIdBuilder("UniRef100_P03923").build())
                .uniref90Id(new UniRefEntryIdBuilder("UniRef90_P03943").build())
                .uniref50Id(new UniRefEntryIdBuilder("UniRef50_P03973").build())
                .isSeed(true)
                .sequence(sequence)
                .build();
    }
}
