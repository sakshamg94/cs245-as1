package memstore.table;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import memstore.data.ByteFormat;
import memstore.data.DataLoader;
//import sun.reflect.generics.tree.Tree;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
//import java.util.Map;
import java.util.TreeMap;

/**
 * Custom table implementation to adapt to provided query mix.
 */
public class CustomTable implements Table {
    int numCols;
    int numRows;
    private TreeMap<Integer, IntArrayList> indexCol0;
    private TreeMap<Integer, TreeMap<Integer, Long>> multiIndexCol12SumCol0;
    private ByteBuffer rows;
    private ByteBuffer col3PlusCol2;
    private ByteBuffer col0;

    public CustomTable() {
    }

    /**
     * HELPER METHOD
     * Adds an int value (row id) to the treemap with key k.
     *
     * @param val is the row id to be added corresponding to col0 value
     * @param k   is the col0 value which serves as the key for this index
     */
    public void addValIndex0(int val, int k) {
        IntArrayList correspond_rows;
        if (!this.indexCol0.containsKey(k)) {
            correspond_rows = new IntArrayList();
        } else {
            correspond_rows = this.indexCol0.get(k);
        }
        correspond_rows.add(val);
        this.indexCol0.put(k, correspond_rows);
    }

    /**
     * HELPER METHOD
     * Adds col0 value to the sum of col0's corresponding to a particular tuple of col1 and col2 values
     *
     * @param col0_value is the col0 value for this row
     * @param col1_value  is the col1 value for that row : primary key for the multiindex
     * @param col2_value is the col2 value for that row : secondary key for the multiindex
     */
    public void addValMultiIndex(int col0_value, int col1_value, int col2_value) {
        // col1_value; primary key for the multiindex
        TreeMap<Integer, Long> correspond_col2_Values;

        if (!this.multiIndexCol12SumCol0.containsKey(col1_value)) {
            correspond_col2_Values = new TreeMap<>();
            correspond_col2_Values.put(col2_value, (long)col0_value);

        } else {
            correspond_col2_Values = this.multiIndexCol12SumCol0.get(col1_value);
            long col0SumSoFar;
            if (!correspond_col2_Values.containsKey(col2_value)) {
                col0SumSoFar = col0_value;
            } else {
                col0SumSoFar = correspond_col2_Values.get(col2_value);
                col0SumSoFar += col0_value;
            }
            correspond_col2_Values.put(col2_value, col0SumSoFar);
            // did the change propagate to the treemap or just
            // locally to the intArrayList ? YES CHECKED
        }
        this.multiIndexCol12SumCol0.put(col1_value, correspond_col2_Values);
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
        this.indexCol0 = new TreeMap<>();
        this.multiIndexCol12SumCol0 = new TreeMap<>();

        this.numCols = loader.getNumCols();
        List<ByteBuffer> rows = loader.getRows();
        numRows = rows.size();

        this.rows = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows * numCols);
        this.col3PlusCol2 = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows);
        this.col0 = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows);

        for (int rowId = 0; rowId < numRows; rowId++) {
            ByteBuffer curRow = rows.get(rowId);
            for (int colId = 0; colId < numCols; colId++) {
                int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
                int col_value = curRow.getInt(ByteFormat.FIELD_LEN * colId);

                // put the value in the table
                this.rows.putInt(offset, col_value);

                // check for any indices that need updates
                if (colId == 0) {
                    this.col0.putInt(ByteFormat.FIELD_LEN * rowId, col_value);
                    addValIndex0(rowId, col_value);

                } else if (colId == 2) {
                    int col1_value = curRow.getInt(ByteFormat.FIELD_LEN);
                    int col0_value = this.col0.getInt(ByteFormat.FIELD_LEN * rowId);
                    addValMultiIndex(col0_value, col1_value, col_value);

                }else if (colId == 3) {
                    int col2_value = curRow.getInt(ByteFormat.FIELD_LEN * 2);
                    this.col3PlusCol2.putInt(ByteFormat.FIELD_LEN * rowId, col_value + col2_value);
                }

                else{}
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
        /**
         * if colId ==0
         *      update the index0 by removing old linkage and forming a new one
         * if colid==1
         *      figre this rowId's col0 and col2 value
         *      go to the col1 key, find subtree with col2 entry of this row and reduce the sum by col0 value
         *      use helper function to add the col0, NEW col1, col2 values to the multiindex
         * if colid ==2
         *      for multiindex with key as col1 old value that contain subtree with the old col2 value as key,
         *          reduce the sum value by col0 amount
         *          use helper function to add the col0, col1, NEW col2 values to the multiindex
         * else {}
         * update the table
         */
        int row_offset = ByteFormat.FIELD_LEN * rowId * numCols;

        int col1_original_val = this.getIntField(rowId, 1);
        int col2_original_val = this.getIntField(rowId, 2);
        int col0_original_val = this.getIntField(rowId, 0);

        if (colId == 0) {
            // remove rowId from old value's IntArrayList in index
            IntArrayList old_row_list = this.indexCol0.get(col0_original_val);
            old_row_list.rem(rowId);
            this.indexCol0.put(col0_original_val, old_row_list);

            // add rowId to new value's IntArrayList in index
            this.addValIndex0(rowId, field);

        } else if (colId == 1 || colId == 2) {
            // 1
            TreeMap<Integer, Long> correspond = this.multiIndexCol12SumCol0.get(col1_original_val);
            long old_sum = correspond.get(col2_original_val);
            correspond.put(col2_original_val, old_sum - col0_original_val);
            this.multiIndexCol12SumCol0.put(col1_original_val, correspond);
            // 2
            if (colId ==1) {
                addValMultiIndex(col0_original_val, field, col2_original_val);
            } else{
                addValMultiIndex(col0_original_val, col1_original_val, field);
            }

        } else{
        }
        this.rows.putInt(row_offset +ByteFormat.FIELD_LEN*colId,field);
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
        long result = 0;
        for(int rowId= 0; rowId<numRows; rowId++){
            result += this.col0.getInt(ByteFormat.FIELD_LEN*rowId);
        }
        return result;

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
         * 1. search multiindex for all keys > threshold1
         *      for each subtree where the col2_value (key) is < thresold2
         *          add the value to the running sum
         */
        long running_sum = 0;

        if (this.multiIndexCol12SumCol0.higherKey(threshold1) == null){
            return running_sum;
        }
        int k = this.multiIndexCol12SumCol0.higherKey(threshold1);

        while(this.multiIndexCol12SumCol0.containsKey(k)){

            TreeMap<Integer, Long> subtree = this.multiIndexCol12SumCol0.get(k);
            if (subtree.lowerKey(threshold2) == null){
                } else {
                int col2_key = subtree.lowerKey(threshold2);

                while (subtree.containsKey(col2_key)) {
                    running_sum += subtree.get(col2_key);

                    if (subtree.lowerKey(col2_key) == null) {
                        break;
                    }
                    col2_key = subtree.lowerKey(k);
                }
            }
            if (this.multiIndexCol12SumCol0.higherKey(k)==null){
                break;
            }
            k = this.multiIndexCol12SumCol0.higherKey(k);
        }
        return running_sum;
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

        if (this.indexCol0.higherKey(threshold) == null){
            return runningSum;
        }
        int k = this.indexCol0.higherKey(threshold);

        while (this.indexCol0.containsKey(k)) {
            IntArrayList row_list = this.indexCol0.get(k);
            for (int rowId : row_list) {
                for (int colId = 0; colId < numCols; colId++) {
                    int offset = ByteFormat.FIELD_LEN * (rowId * numCols + colId);
                    int col_value = this.rows.getInt(offset);
                    runningSum += col_value;
                }
            }
            if (this.indexCol0.higherKey(k) == null){
                break;
            }
            k = this.indexCol0.higherKey(k);
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

        if (this.indexCol0.lowerKey(threshold) == null) {
            return updatedRows;
        }
        int k = this.indexCol0.lowerKey(threshold);

        while (this.indexCol0.containsKey(k)) {
            IntArrayList row_list = this.indexCol0.get(k);
            for (int rowId : row_list) {
                updatedRows += 1;
                putIntField(rowId,3, this.col3PlusCol2.getInt(ByteFormat.FIELD_LEN * rowId));
            }
            if (this.indexCol0.lowerKey(k) == null) {
                break;
            }
            k = this.indexCol0.lowerKey(k);
        }
        return updatedRows;
    }
}
