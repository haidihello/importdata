package com.example.importdata.util;

import com.alibaba.druid.pool.DruidDataSourceFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.Properties;

public class JdbcUtil {
    private static DataSource dataSource; //数据库连接池
    static {
        Properties properties = new Properties();
        InputStream is = JdbcUtil.class.getClassLoader().getResourceAsStream("application.properties");
        try {
            properties.load(is);
            is.close();
            dataSource = DruidDataSourceFactory.createDataSource(properties);
        }  catch (IOException ioException) {
            ioException.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static Connection getConnection() throws Exception {
        return dataSource.getConnection();
    }
    public static Connection getConnectionOld() throws IOException, ClassNotFoundException, SQLException {
        InputStream is=JdbcUtil.class.getClassLoader().getResourceAsStream("application.properties");
        Properties properties = new Properties();
        properties.load(is);
        is.close();

        String driverClass=properties.getProperty("driverClass");
        String url=properties.getProperty("url");
        String user=properties.getProperty("user");
        String password=properties.getProperty("password");

        Class.forName(driverClass);
        Connection connection = DriverManager.getConnection(url, user, password);
        return connection;

    }

    public static void close(Connection connection){
       close(connection,null,null);
    }

    public static void close(Connection connection, Statement statement){
        close(connection,statement,null);
    }

    public static void close(Connection connection, Statement statement, ResultSet resultSet){
        if(resultSet!=null){
            try {
                resultSet.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
        if(statement!=null){
            try {
                statement.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
        if(connection!=null){
            try {
                connection.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }

}