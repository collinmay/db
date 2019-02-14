package com.collinswebsite.db;

import com.collinswebsite.db.miniql.Expression;
import com.collinswebsite.db.miniql.InvalidExpressionException;
import com.collinswebsite.db.types.IntegerDataType;
import com.collinswebsite.db.types.StringDataType;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

public class SortingCursor implements Cursor {
    private final Cursor base;
    private final List<Expression> fields;

    private List<Row> sortedRows;
    private int readHead = 0;

    public SortingCursor(Cursor base, List<Expression> fields) {
        this.base = base;
        this.fields = fields;
    }

    @Override
    public boolean isAtEnd() throws DeserializationException {
        if(sortedRows == null) {
            sort();
        }
        return readHead >= sortedRows.size();
    }

    @Override
    public Row getNext() throws DeserializationException {
        if(sortedRows == null) {
            sort();
        }
        return sortedRows.get(readHead++);
    }

    @Override
    public Table getTable() {
        return base.getTable();
    }

    @Override
    public CompletionStage<Void> await() {
        return null;
    }

    @Override
    public void setFilter(Expression filter) throws InvalidExpressionException {
        base.setFilter(filter);
    }

    private void sort() throws DeserializationException {
        List<Row> input = new ArrayList<>();
        while(!base.isAtEnd()) {
            input.add(base.getNext()); // NOTE: not safe for async
        }

        input.sort((a, b) -> {
            for(Expression e : fields) {
                if(e.getType() instanceof IntegerDataType) {
                    long r = e.evaluateAsInteger(a) - e.evaluateAsInteger(b);
                    if(r > 0) { return 1; }
                    if(r < 0) { return -1; }
                } else if(e.getType() instanceof StringDataType) {
                    String aStr = (String) e.evaluate(a);
                    String bStr = (String) e.evaluate(b);
                    int r = aStr.compareTo(bStr);
                    if(r != 0) { return r; }
                }
            }
            return 0;
        });

        sortedRows = input;
    }
}
