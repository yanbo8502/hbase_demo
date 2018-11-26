package hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.util.concurrent.Callable;

/**
 * Created by yanbo on 18-11-23.
 */
class DataIO implements Callable {

    private long range_start;
    private long range_end;
    private Table table;

    public DataIO(long range_start, long range_end, Table table) {
        this.range_end = range_end;
        this.range_start = range_start;
        this.table = table;
    }


    @Override
    public Object call() throws Exception {
        for (Long i = range_start; i < range_end; i++) {

            try {
                String phone = String.format("13%09d", i);
                String cid = String.format("c%d", i);
                String id = i.toString();
                String rowKey = String.format("%s_%s_%s", id, cid, phone);
                //System.out.println(rowKey);
                Put put = new Put(rowKey.getBytes());
                put.addColumn("f1".getBytes(), "id".getBytes(), id.getBytes());
                put.addColumn("f1".getBytes(), "cid".getBytes(), cid.getBytes());
                put.addColumn("f1".getBytes(), "phone".getBytes(), phone.getBytes());
                put.addColumn("f2".getBytes(), "data".getBytes(), i.toString().getBytes());
                table.put(put);

                // Close your table and cluster connection.
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } finally {
            }

        }
        return true;
    }
}
