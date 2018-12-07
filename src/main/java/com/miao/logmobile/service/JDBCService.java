package com.miao.logmobile.service;

import com.miao.logmobile.common.JDBCConnection;

import java.sql.*;

public class JDBCService {

    private static Connection connection;

    static {

        try {
            Class.forName(JDBCConnection.JDBC_DRIVER);

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static Connection getConnection(){
        try {
            if(connection==null||connection.isClosed()) {
                connection = DriverManager.getConnection(JDBCConnection.JDBC_URL, JDBCConnection.USERNAME, JDBCConnection.PASSWORD);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
        return connection;
    }

    public static void colseAll(Connection connection, PreparedStatement preparedStatement, ResultSet resultSet) {
        try {
            if(resultSet!=null){
                resultSet.close();
            }
            if(preparedStatement!=null){
                preparedStatement.close();
            }
            if(connection!=null){
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
