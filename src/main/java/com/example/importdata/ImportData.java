package com.example.importdata;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.druid.util.JdbcUtils;
import com.example.importdata.util.FileSplitAndCombine;
import com.example.importdata.util.JdbcUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class ImportData {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    public void loadDataInfile() {
        String sql = "LOAD DATA LOCAL INFILE 'C:/Users/86176/Desktop/temp/check/ylwx2.csv' INTO TABLE order_data \n" +
                "FIELDS TERMINATED BY ',`'\n" +
                "LINES TERMINATED BY '\\n'  \n" +
                "IGNORE 1 lines\n" +
                "(@col1,@col2,@col3,@col4,@col5,@col6,@col7,@col8,@col9,@col10,@col11,@col12,@col13,@col14,@col15,@col16,@col17,@col18,@col19,@col20,@col21,@col22,@col23,@col24,@col25,@col26,@col27,@col28) \n" +
                "set transTime=substring(@col1,2,19),unionpayNum=@col3, ,systemNo=@col7,tranStatus=@col10, transAmt=@col13;";
        jdbcTemplate.execute(sql);
    }

    /**
     * 文件最后两行为统计数据 舍去
     * 总计2631141行数据
     */
//    @PostConstruct
    public void importDataLoad() {
        Long stat = System.currentTimeMillis();
        String source = "C:/Users/86176/Desktop/temp/check/ylwx";
        int fileCount = FileSplitAndCombine.splitLargeFile(source, "csv", 450000, false);

        System.out.println("切分文件数量" + fileCount);
        ExecutorService service = Executors.newFixedThreadPool(fileCount);
        for (int i = 0; i <= fileCount; i++) {
            String newTxt0 = "C:/Users/86176/Desktop/temp/check/ylwx_" + i + ".txt";
            service.submit(() -> {
                String importsql = "LOAD DATA LOCAL INFILE " +
                        "'" + newTxt0 + "' " +
                        "INTO TABLE order_data " +
                        "FIELDS TERMINATED BY ','\n" +
                        "LINES TERMINATED BY '\\n'\n" +
                        "(@col1,@col2,@col3,@col4,@col5) \n" +
                        "set transTime=@col1,unionpayNum=@col2, systemNo=@col3,tranStatus=@col4, transAmt=@col15;";
                System.out.println(importsql);
                jdbcTemplate.execute(importsql);
            });
        }

        System.out.println("总耗时" + String.valueOf(System.currentTimeMillis() - stat));
    }

//    @PostConstruct
    public void insertSync() throws IOException {
        Long stat = System.currentTimeMillis();
        String path = "C:\\Users\\86176\\Desktop\\temp\\check\\ylwx.csv";
        LineIterator it = FileUtils.lineIterator(new File(path), "UTF-8");
        int count = 0;
        ExecutorService service = Executors.newFixedThreadPool(100);
        while (it.hasNext()) {
            String str = it.nextLine();
            if (!str.startsWith("`20")) {
                continue;
            }
            str = str.replaceAll("`", "");
            String[] strArr = str.split(",");
            String insertSql = "insert into order_data set"
                    + " transTime = " + "'" + strArr[0]+ "'"
                    + ",unionpayNum=" + "'" + strArr[2] + "'"
                    + ",systemNo=" + "'" + strArr[6] + "'"
                    + ",tranStatus=" + "'" + strArr[9] + "'"
                    + ",transAmt=" + "'" + strArr[12] + "'";
            count++;
            service.submit(() -> {
                jdbcTemplate.execute(insertSql);
            });
        }
        System.out.println("总笔数" + count);
        System.out.println("总耗时" + String.valueOf(System.currentTimeMillis() - stat));
    }

    @PostConstruct
    public void batchInsert() throws Exception {
        Long stat = System.currentTimeMillis();
        String path = "C:\\Users\\86176\\Desktop\\temp\\check\\ylwx.csv";
        LineIterator it = FileUtils.lineIterator(new File(path), "UTF-8");
        int count = 0;
        Connection connection = JdbcUtil.getConnection();
        String insertSql = "";
        PreparedStatement preparedStatement = Objects.requireNonNull(connection).prepareStatement(insertSql);
        connection.setAutoCommit(false);
        while (it.hasNext()) {
            String str = it.nextLine();
            if (!str.startsWith("`20")) {
                continue;
            }
            count++;
            str = str.replaceAll("`", "");
            String[] strArr = str.split(",");
            insertSql = "insert into order_data set"
                    + " transTime = " + "'" + strArr[0]+ "'"
                    + ",unionpayNum=" + "'" + strArr[2] + "'"
                    + ",systemNo=" + "'" + strArr[6] + "'"
                    + ",tranStatus=" + "'" + strArr[9] + "'"
                    + ",transAmt=" + "'" + strArr[12] + "'";
            preparedStatement.addBatch();
            if (count % 1000 == 0) {
                preparedStatement.executeBatch();
                preparedStatement.clearBatch();
            }

        }
        connection.commit();
        System.out.println("总笔数" + count);
        System.out.println("总耗时" + String.valueOf(System.currentTimeMillis() - stat));
    }
}
