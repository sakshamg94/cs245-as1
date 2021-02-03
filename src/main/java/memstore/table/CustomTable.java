package memstore.table;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import memstore.data.ByteFormat;
import memstore.data.DataLoader;
//import sun.reflect.generics.tree.Tree;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
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
    private TreeMap<Integer, IntArrayList> indexCol1;
    private TreeMap<Integer, IntArrayList> indexCol2;

    private long sumCol0 = 0;

    private ByteBuffer rows;
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
     * Adds an int value (row id) to the treemap with key k.
     *
     * @param val is the row id to be added corresponding to col0 value
     * @param k   is the col0 value which serves as the key for this index
     */
    public void addValIndex1(int val, int k) {
        IntArrayList correspond_rows;
        if (!this.indexCol1.containsKey(k)) {
            correspond_rows = new IntArrayList();
        } else {
            correspond_rows = this.indexCol1.get(k);
        }
        correspond_rows.add(val);
        this.indexCol1.put(k, correspond_rows);
    }

    /**
     * HELPER METHOD
     * Adds an int value (row id) to the treemap with key k.
     *
     * @param val is the row id to be added corresponding to col0 value
     * @param k   is the col0 value which serves as the key for this index
     */
    public void addValIndex2(int val, int k) {
        IntArrayList correspond_rows;
        if (!this.indexCol2.containsKey(k)) {
            correspond_rows = new IntArrayList();
        } else {
            correspond_rows = this.indexCol2.get(k);
        }
        correspond_rows.add(val);
        this.indexCol2.put(k, correspond_rows);
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
        this.indexCol1 = new TreeMap<>();
        this.indexCol2 = new TreeMap<>();

        this.numCols = loader.getNumCols();
        List<ByteBuffer> rows = loader.getRows();
        numRows = rows.size();

        this.rows = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows * numCols);
        this.col0 = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows);

        for (int rowId = 0; rowId < numRows; rowId++) {
            ByteBuffer curRow = rows.get(rowId);
            long running_sum = 0;
            for (int colId = 0; colId < numCols; colId++) {
                int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
                int col_value = curRow.getInt(ByteFormat.FIELD_LEN * colId);
                running_sum+=col_value;

                // put the value in the table
                this.rows.putInt(offset, col_value);

                // check for any indices that need updates
                if (colId == 0) {
                    this.col0.putInt(ByteFormat.FIELD_LEN * rowId, col_value);
                    addValIndex0(rowId, col_value);
                    this.sumCol0+=col_value;

                } else if (colId == 1) {
                    addValIndex1(rowId, col_value);

                } else if(colId==2){
                    addValIndex2(rowId, col_value);
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
        if (colId==0){
            return this.col0.getInt(ByteFormat.FIELD_LEN*rowId);
        }
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
         * if colId ==0 or 1 or 2
         *      update the index0 by removing old linkage and forming a new one,
         *      must also change value at col0 in colid==0
         *
         * else {}
         * update the table
         */
        int row_offset = ByteFormat.FIELD_LEN * rowId * numCols;

        if (colId == 0) {
            int col0_original_val = this.getIntField(rowId, 0);
            // remove rowId from old value's IntArrayList in index
            IntArrayList old_row_list = this.indexCol0.get(col0_original_val);
            old_row_list.rem(rowId);
            this.indexCol0.put(col0_original_val, old_row_list);

            // add rowId to new value's IntArrayList in index
            this.addValIndex0(rowId, field);

            // update in col0 buffer
            this.col0.putInt(ByteFormat.FIELD_LEN*rowId);

            // update the pre computed sum
            this.sumCol0-=col0_original_val;
            this.sumCol0+=field;


        } else if (colId == 1) {
            // 1
            int col1_original_val = this.getIntField(rowId, 1);
            // remove rowId from old value's IntArrayList in index
            IntArrayList old_row_list = this.indexCol1.get(col1_original_val);
            old_row_list.rem(rowId);
            this.indexCol1.put(col1_original_val, old_row_list);

            // add rowId to new value's IntArrayList in index
            this.addValIndex1(rowId, field);

        } else if (colId==2){
            // 1
            int col2_original_val = this.getIntField(rowId, 2);
            // remove rowId from old value's IntArrayList in index
            IntArrayList old_row_list = this.indexCol2.get(col2_original_val);
            old_row_list.rem(rowId);
            this.indexCol2.put(col2_original_val, old_row_list);

            // add rowId to new value's IntArrayList in index
            this.addValIndex2(rowId, field);
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
        return this.sumCol0;

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
         * Set together all rows that satisfy col1> threshold1
         * Set together all rows that satisfy col2< threshold2
         * iterate over the smaller of the 2 sets by checking for presence in the larger one O(1)
         *      sum col0 entries into running sum
         */
        long running_sum = 0;

        // get rows for first col
        if (this.indexCol1.higherKey(threshold1) == null || this.indexCol2.lowerKey(threshold2)==null){
            return running_sum;
        }
        int k = this.indexCol1.higherKey(threshold1);

        HashSet<Integer> col1_satisfied = new HashSet<>();

        while (this.indexCol1.containsKey(k)) {
            IntArrayList row_list = this.indexCol1.get(k);
            col1_satisfied.addAll(row_list);

            if (this.indexCol1.higherKey(k) == null) {
                break;
            }
            k = this.indexCol1.higherKey(k);
        }
         // get rows for second col
        k = this.indexCol2.lowerKey(threshold2);

        HashSet<Integer> col2_satisfied = new HashSet<>();

        while (this.indexCol2.containsKey(k)) {
            IntArrayList row_list = this.indexCol2.get(k);
            col2_satisfied.addAll(row_list);

            if (this.indexCol2.lowerKey(k) == null) {
                break;
            }
            k = this.indexCol2.lowerKey(k);
        }
        // only contain the intersection of rows
        col1_satisfied.retainAll(col2_satisfied);

        // calculate the sum
        for (Integer rowId : col1_satisfied){
            running_sum+=this.col0.getInt(ByteFormat.FIELD_LEN *rowId);
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
                    int col_value = getIntField(rowId, colId);
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
                putIntField(rowId,3, getIntField(rowId,3) + getIntField(rowId,2));
            }
            if (this.indexCol0.lowerKey(k) == null) {
                break;
            }
            k = this.indexCol0.lowerKey(k);
        }
        return updatedRows;
    }
}
