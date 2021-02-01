package memstore.table;

import memstore.data.ByteFormat;
import memstore.data.DataLoader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * RowTable, which stores data in row-major format.
 * That is, data is laid out like
 *   row 1 | row 2 | ... | row n.
 */
public class RowTable implements Table {
    protected int numCols;
    protected int numRows;
    protected ByteBuffer rows;

    public RowTable() { }

    /**
     * Loads data into the table through passed-in data loader. Is not timed.
     *
     * @param loader Loader to load data from.
     * @throws IOException
     */
    @Override
    public void load(DataLoader loader) throws IOException {
        this.numCols = loader.getNumCols();
        List<ByteBuffer> rows = loader.getRows();
        numRows = rows.size();
        this.rows = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows * numCols);

        for (int rowId = 0; rowId < numRows; rowId++) {
            ByteBuffer curRow = rows.get(rowId);
            for (int colId = 0; colId < numCols; colId++) {
                int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
                this.rows.putInt(offset, curRow.getInt(ByteFormat.FIELD_LEN * colId));
            }
        }
    }

    /**
     * Returns the int field at row `rowId` and column `colId`.
     */
    @Override
    public int getIntField(int rowId, int colId) {
        // TODO: Implement this!
//        if (rowId < 0 || rowId > numRows-1 || colId > numCols -1 || colId < 0){
//            throw new IOException("invalid row or col Id");
//        } else{
        int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
        return this.rows.getInt(offset);
//        }
    }

    /**
     * Inserts the passed-in int field at row `rowId` and column `colId`.
     */
    @Override
    public void putIntField(int rowId, int colId, int field) {
        // TODO: Implement this!
        int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
        this.rows.putInt(offset, field);
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) FROM table;
     *
     *  Returns the sum of all elements in the first column of the table.
     */
    @Override
    public long columnSum() {
        // TODO: Implement this!
        long required_sum = 0;
        for (int rowId = 0; rowId < numRows; rowId++) {
            int offset = ByteFormat.FIELD_LEN*rowId*numCols;
            required_sum+=this.rows.getInt(offset);
        }
        return required_sum;
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) FROM table WHERE col1 > threshold1 AND col2 < threshold2;
     *
     *  Returns the sum of all elements in the first column of the table,
     *  subject to the passed-in predicates.
     */
    @Override
    public long predicatedColumnSum(int threshold1, int threshold2) {
        // TODO: Implement this!
        long required_sum = 0;
        for (int rowId = 0; rowId < numRows; rowId++) {
            int row_offset = ByteFormat.FIELD_LEN * rowId * numCols;
            int col1_val = this.rows.getInt(row_offset + ByteFormat.FIELD_LEN);
            if (col1_val > threshold1) {
                int col2_val = this.rows.getInt(row_offset + 2*ByteFormat.FIELD_LEN);
                if (col2_val < threshold2) {
                    required_sum += this.rows.getInt(row_offset);
                }
            }
        }
        return required_sum;
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) + SUM(col1) + ... + SUM(coln) FROM table WHERE col0 > threshold;
     *
     *  Returns the sum of all elements in the rows which pass the predicate.
     */
    @Override
    public long predicatedAllColumnsSum(int threshold) {
        // TODO: Implement this!
        long runningSum = 0;
        for (int rowId = 0; rowId < numRows; rowId++){
            int row_offset = ByteFormat.FIELD_LEN*rowId*numCols;
            int col0_val = this.rows.getInt(row_offset);
            if (col0_val > threshold){
                for (int colId = 0; colId < numCols; colId++){
                    runningSum += this.rows.getInt(row_offset + ByteFormat.FIELD_LEN*colId);
                }
            }
        }
        return runningSum;
    }

    /**
     * Implements the query
     *   UPDATE(col3 = col3 + col2) WHERE col0 < threshold;
     *
     *   Returns the number of rows updated.
     */
    @Override
    public int predicatedUpdate(int threshold) {
        // TODO: Implement this!
        int updatedRows = 0;
        for (int rowId = 0; rowId < numRows; rowId++) {
            int row_offset = ByteFormat.FIELD_LEN * rowId * numCols;
            int col0_val = this.rows.getInt(row_offset);
            if (col0_val < threshold) {
                updatedRows += 1;
                int col3_val = this.rows.getInt(row_offset + ByteFormat.FIELD_LEN * 3);
                int col2_val = this.rows.getInt(row_offset + ByteFormat.FIELD_LEN * 2);
                this.rows.putInt(row_offset + ByteFormat.FIELD_LEN *3, col3_val + col2_val);
            }
        }
        return updatedRows;
    }
}
