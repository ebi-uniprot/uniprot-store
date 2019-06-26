package uk.ac.ebi.uniprot.search.expr;

import lombok.Builder;
import lombok.Getter;

/**
 * Created 26/06/19
 *
 * @author Edd
 */
@Builder
@Getter
public class BoostExpression {
    private String expression;
    private Float boost;
}
