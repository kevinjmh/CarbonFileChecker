package carbon;

import org.apache.carbondata.core.reader.CarbonIndexFileReader;
import org.apache.carbondata.format.*;

import java.io.*;
import java.util.List;

public class IndexReader {

    public static void raw(String filePath) throws IOException {
        CarbonIndexFileReader indexReader = new CarbonIndexFileReader();
        try {
            // open the reader
            indexReader.openThriftReader(filePath);
            // get the index header
            org.apache.carbondata.format.IndexHeader readIndexHeader = indexReader.readIndexHeader();
            List<org.apache.carbondata.format.ColumnSchema> table_columns =
                    readIndexHeader.getTable_columns();
            // read the block info from file
            while (indexReader.hasNext()) {
                BlockIndex readBlockIndexInfo = indexReader.readBlockIndexInfo();
                long rows = readBlockIndexInfo.getNum_rows();
                BlockletIndex bi = readBlockIndexInfo.getBlock_index();
                System.out.println();
            }
        } finally {
            indexReader.closeThriftReader();
        }
    }
    public static void main(String[] args) throws IOException {
        raw("C:\\Users\\Manhua\\Documents\\WeChat Files\\Man_Hua\\Files\\12_batchno0-0-0-1563606492537.carbonindex");
    }
}
