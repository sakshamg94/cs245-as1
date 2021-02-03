package memstore.table;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import memstore.data.ByteFormat;
import memstore.data.DataLoader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.TreeMap;

/**
 * IndexedRowTable, which stores data in row-major format.
 * That is, data is laid out like
 *   row 1 | row 2 | ... | row n.
 *
 * Also has a tree index on column `indexColumn`, which points
 * to all row indices with the given value.
 */
public class IndexedRowTable implements Table {

    int numCols;
    int numRows;
    private TreeMap<Integer, IntArrayList> index;
    private ByteBuffer rows;
    private int indexColumn;

    public IndexedRowTable(int indexColumn) {
        this.indexColumn = indexColumn;
    }

    /** HELPER METHOD
     * Adds an int value (row id) to the treemap with key k.
     * @param val is the row id to be added corresponding to col0 value
     * @param k is the col0 value which serves as the key for this index
     */
    public void addVal(int val, int k) {
        IntArrayList correspond_rows;
        if (!this.index.containsKey(k)) {
            correspond_rows = new IntArrayList();
        } else {
            correspond_rows = this.index.get(k);
        }
        correspond_rows.add(val);
        this.index.put(k, correspond_rows);
    }

    /**
     * Loads data into the table through passed-in data loader. Is not timed.
     *
     * @param loader Loader to load data from.
     * @throws IOException
     */
    @Override
    public void load(DataLoader loader) throws IOException {
        // TODO: Implement this!
        this.index = new TreeMap<Integer, IntArrayList>();
        this.numCols = loader.getNumCols();
        List<ByteBuffer> rows = loader.getRows();
        numRows = rows.size();
        this.rows = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows * numCols);

        for (int rowId = 0; rowId < numRows; rowId++) {
            ByteBuffer curRow = rows.get(rowId);
            for (int colId = 0; colId < numCols; colId++) {
                int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
                int col_value = curRow.getInt(ByteFormat.FIELD_LEN * colId);
                this.rows.putInt(offset, col_value);
                if(colId==this.indexColumn){
                    addVal(rowId, col_value);
                }
            }
        }
    }

    /**
     * Returns the int field at row `rowId` and column `colId`.
     */
    @Override
    public int getIntField(int rowId, int colId) {
        // TODO: Implement this!
        int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
        return this.rows.getInt(offset);
    }

    /**
     * Inserts the passed-in int field at row `rowId` and column `colId`.
     */
    @Override
    public void putIntField(int rowId, int colId, int field) {
        // TODO: Implement this!

        if(colId==this.indexColumn) {
            // remove rowId from old value's IntArrayList in index
            int old_value = getIntField(rowId, colId);
            IntArrayList old_row_list = this.index.get(old_value);
            old_row_list.rem(rowId);
            this.index.put(old_value, old_row_list);

            // add rowId to new value's IntArrayList in index
            addVal(rowId, field);
        }
        // finally add the value to the table
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
        /**
         * Check which of the 2 cols is indexColumn if any
         * If there is an indexColumn then
         * 1. create a loop that iterates through all the index keys >/< the threshold
         *      Create a subloop to iterate through all the rowIds of a key
         *          check for the alternate condition and make addition to the sum
         * 2. If neither is an indexColumn, then paste the code from rowTable.java
         */
        long required_sum = 0;

        if (this.indexColumn == 1){ // col1 is the indexed col
            if (this.index.higherKey(threshold1) == null){
                return required_sum;
            }
            int k = this.index.higherKey(threshold1);

            while(this.index.containsKey(k)){
                IntArrayList row_list = this.index.get(k);
                for(int rowId : row_list){
                    int col2_value = getIntField(rowId, 2);
                    if (col2_value < threshold2){
                        required_sum+=col2_value;
                    }
                }
                if (this.index.higherKey(k) == null){
                    break;
                }
                k = this.index.higherKey(k);
            }

        }else if (this.indexColumn ==2){ // col2 is the indexed column
            if (this.index.lowerKey(threshold2) == null){
                return required_sum;
            }
            int k = this.index.lowerKey(threshold2);

            while(this.index.containsKey(k)){
                IntArrayList row_list = this.index.get(k);
                for(int rowId : row_list){
                    int col1_value = getIntField(rowId, 1);
                    if (col1_value > threshold1){
                        required_sum+=col1_value;
                    }
                }
                if (this.index.lowerKey(k) == null){
                    break;
                }
                k = this.index.lowerKey(k);
            }

        }else{ // function same as that for rowTable.java
            for (int rowId = 0; rowId < numRows; rowId++) {
                int col1_val = getIntField(rowId, 1);
                if (col1_val > threshold1) {
                    int col2_val = getIntField(rowId,2);
                    if (col2_val < threshold2) {
                        required_sum += getIntField(rowId, 0);
                    }
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

        if (this.indexColumn == 0) { // col0 is the indexed col
            if (this.index.higherKey(threshold) == null){
                return runningSum;
            }
            int k = this.index.higherKey(threshold);

            while (this.index.containsKey(k)) {
                IntArrayList row_list = this.index.get(k);
                for (int rowId : row_list) {
                    for (int colId = 0; colId < numCols; colId++) {
                        int col_value = getIntField(rowId, colId);
                        runningSum += col_value;
                    }
                }
                if (this.index.higherKey(k) == null){
                    break;
                }
                k = this.index.higherKey(k);
            }

        } else{ // same method as for rowTable.java
            for (int rowId = 0; rowId < numRows; rowId++){
                int col0_val = getIntField(rowId, 0);
                if (col0_val > threshold){
                    for (int colId = 0; colId < numCols; colId++){
                        runningSum += getIntField(rowId, colId);
                    }
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

        if (this.indexColumn == 0){
            if (this.index.lowerKey(threshold) == null){
                return  updatedRows;
            }
            int k = this.index.lowerKey(threshold);

            while (this.index.containsKey(k)) {
                IntArrayList row_list = this.index.get(k);
                for (int rowId : row_list) {
                    updatedRows+=1;
                    int col3_value = getIntField(rowId, 3);
                    int col2_value = getIntField(rowId, 2);
                    putIntField(rowId, 3, col2_value + col3_value);
                }
                if (this.index.lowerKey(k) == null){
                    break;
                }
                k = this.index.lowerKey(k);
            }
        }else if (this.indexColumn == 3){
            for (int rowId = 0; rowId < numRows; rowId++) {

                int col0_val = getIntField(rowId, 0);

                if (col0_val < threshold) {
                    updatedRows += 1;

                    int col3_val = getIntField(rowId, 3);
                    // remove the rowId in the index with key as the col3's old value
                    IntArrayList old_row_list = this.index.get(col3_val);
                    old_row_list.rem(rowId);
                    this.index.put(col3_val, old_row_list);

                    // get the col2 value for this row
                    int col2_val = getIntField(rowId, 2);

                    // insert the rowId in the index with key as the col3's new value
                    // finally insert the new col3 value in the table as well
                    putIntField(rowId, 3, col3_val + col2_val);
                }
            }
        }else  {
            for (int rowId = 0; rowId < numRows; rowId++) {
                int col0_val = getIntField(rowId, 0);
                if (col0_val < threshold) {
                    updatedRows += 1;
                    int col3_val = getIntField(rowId, 3);
                    int col2_val = getIntField(rowId, 2);
                    putIntField(rowId, 3, col3_val + col2_val);
                }
            }
        }
        return updatedRows;
    }
}
