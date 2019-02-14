package com.collinswebsite.db.miniql;

import com.collinswebsite.db.Row;
import com.collinswebsite.db.types.BooleanDataType;
import com.collinswebsite.db.types.DataType;
import org.antlr.v4.runtime.misc.ParseCancellationException;

public class ComparisonExpression implements Expression {
    private Expression left;
    private Operator operator;
    private Expression right;

    public static Operator getOperator(MiniQLParser.ComparatorContext op) {
        if(op.EQ() != null) { return Operator.EQUAL; }
        if(op.NE() != null) { return Operator.NOT_EQUAL; }
        if(op.LT() != null) { return Operator.LESS_THAN; }
        if(op.GT() != null) { return Operator.GREATER_THAN; }

        throw new ParseCancellationException("unrecognized operator: " + op.getText());
    }

    public enum Operator {
        LESS_THAN, GREATER_THAN, EQUAL, NOT_EQUAL;

        public boolean canCompare(DataType left, DataType right) {
            return left.isComparable(right);
        }
    }

    public ComparisonExpression(Expression left, Operator op, Expression right) {
        this.left = left;
        this.operator = op;
        this.right = right;
    }

    @Override
    public DataType getType() {
        return BooleanDataType.DEFAULT;
    }

    @Override
    public Object evaluate(Row r) {
        return evaluateAsBoolean(r);
    }

    @Override
    public boolean evaluateAsBoolean(Row r) {
        switch(operator) {
            case LESS_THAN:
                return left.evaluateAsInteger(r) < right.evaluateAsInteger(r);
            case GREATER_THAN:
                return left.evaluateAsInteger(r) > right.evaluateAsInteger(r);
            case EQUAL:
                return left.evaluate(r).equals(right.evaluate(r));
            case NOT_EQUAL:
                return !(left.evaluate(r).equals(right.evaluate(r)));
        }
        throw new IllegalStateException("invalid operator");
    }

    @Override
    public void validate() throws InvalidExpressionException {
        left.validate();
        right.validate();
        if(!operator.canCompare(left.getType(), right.getType())) {
            throw new InvalidExpressionException("can't compare " + left.getType() + " with " + right.getType());
        }
    }
}
