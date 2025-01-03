package org.uniprot.store.search.field.validator;

import java.util.regex.Pattern;

/**
 * This class defines regular expressions as constants, for reference by other classes if needed.
 * Note that this provides static, compile time access to some regular expressions for fields.
 * However, the defacto source is {@link
 * org.uniprot.store.config.searchfield.common.SearchFieldConfig}, via the {@link
 * org.uniprot.store.config.searchfield.common.SearchFieldConfig#isSearchFieldValueValid(String,
 * String)} (String, String)} method.
 *
 * @author lgonzales
 */
public class FieldRegexConstants {
    public static final String UNIPROTKB_ACCESSION_REGEX =
            "([OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z]([0-9][A-Z][A-Z0-9]{2}){1,2}[0-9])(-[0-9]+)?";
    public static final String UNIPROTKB_ACCESSION_OR_ID =
            "([OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z]([0-9][A-Z][A-Z0-9]{2}){1,2}[0-9])(((-[0-9]{1,3})?(\\[\\d+-\\d+\\])?)|(\\.[0-9]{1,3})|(_[A-Z0-9]{2,5}))?|[A-Z0-9]{2,5}_[A-Z0-9]{2,5}";

    public static final Pattern UNIPROTKB_ACCESSION_SEQUENCE_RANGE_REGEX =
            Pattern.compile(UNIPROTKB_ACCESSION_REGEX + "(\\[\\d+-\\d+\\])");

    public static final Pattern UNIPROTKB_ACCESSION_OPTIONAL_SEQ_RANGE =
            Pattern.compile(
                    "([OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z]([0-9][A-Z][A-Z0-9]{2}){1,2}[0-9])((-[0-9]{1,3})?)(\\[\\d+-\\d+\\])?");

    public static final String PROTEOME_ID_REGEX = "UP[0-9]{9}";
    public static final String UNIPARC_UPI_REGEX = "UPI[\\w]{10}";
    public static final Pattern UNIPARC_UPI_OPTIONAL_SEQ_RANGE =
            Pattern.compile("UPI[\\w]{10}(\\[\\d+-\\d+\\])?");
    public static final Pattern UNIPARC_UPI_SEQUENCE_RANGE_REGEX =
            Pattern.compile(UNIPARC_UPI_REGEX + "(\\[\\d+-\\d+\\])");
    public static final String UNIREF_CLUSTER_ID_REGEX =
            "(UniRef100|UniRef90|UniRef50)_\\w+(-[0-9]+)?";
    public static final String SEQUENCE_REGEX = "^[A-Z]+$|^NULL$";
    public static final String COMMA_SEPARATED_REGEX = "\\s*,\\s*";
    public static final String CLEAN_QUERY_REGEX = "(?:^\\()|(?:\\)$)";
    public static final String GO_ID_REGEX = "^GO:\\d{7}$";
    public static final String EC_ID_REGEX =
            "^([1-7])\\.("
                    + "((\\d{1,2})\\.(\\d{1,2})\\.(\\d{1,3}|n\\d{1,2}|-))|"
                    + "((\\d{1,2})\\.(\\d{1,2}|-)\\.-)|"
                    + "((\\d{1,2}|-)\\.-\\.-)"
                    + ")$";
    public static final String KEYWORD_ID_REGEX = "^KW-\\d{4}$";
    public static final String TAXONOMY_ID_REGEX = "^\\d+$";
    public static final String CROSS_REF_REGEX = "DB-(\\d{4})";
    public static final String DISEASE_REGEX = "DI-(\\d{5})";

    public static final String ARBA_ID_REGEX = "ARBA(\\d{8})";

    public static final String UNIRULE_ALL_ID_REGEX =
            "UR[0-9]{9}|MF_[0-9]{5}|PIRSR[0-9]+(\\-[0-9]+)?|PIRNR[0-9]+|RU[0-9]{6}|PRU[0-9]{5}";
    public static final String UNIRULE_ID_REGEX = "UR[0-9]{9}";

    public static final String SUBCELLULAR_LOCATION_ID_REGEX = "^SL-[0-9]{4}";

    private FieldRegexConstants() {}
}
