package com.ctrip.hermes.broker.mysql;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Pattern;

import org.junit.Test;
import org.unidal.helper.Files;

public class CreateTables {
    private final String DATABASE_NAME = "hermes";
    private String sqlFilePath = "/mysql/";

    // mysql config:
    private final String JDBC_URL = "jdbc:mysql://127.0.0.1:3306";
    private final String MYSQL_USER = "root";
    private final String MYSQL_USER_PSW = "123456";


    private String TOPIC = "testtopic";

    private String m_path = "/data/appdatas/paas";


    @Test
    public void setupMysql() throws Exception {
        System.out.println("Starting to MySQL setup ...");

        setupDatabase();

        System.out.println("MySQL setup is finished!");
    }


    private boolean setupDatabase() {
        Connection conn = null;
        Statement stmt = null;
        boolean isSuccess = false;

        try {
            System.out.println("Connecting to database(" + JDBC_URL + ") ...");
            conn = getConnection(JDBC_URL);
            System.out.println("Connected to database(" + JDBC_URL + ")");

            System.out.println("Creating database " + DATABASE_NAME);
            stmt = conn.createStatement();

            createDatabase(stmt);
            System.out.println("Database" + DATABASE_NAME + " is created successfully");

            System.out.println("Creating tables ...");

            createTables(stmt);
            System.out.println("Tables are created successfully");

            isSuccess = true;
        } catch (Exception e) {
            System.err.println(e);
            throw new RuntimeException(e.getMessage());
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception e) {
                // ignore it
            }
        }
        return isSuccess;
    }


    private void createDatabase(Statement stmt) throws SQLException {
        try {
            stmt.executeUpdate("drop database if exists " + DATABASE_NAME);
            // TODO: 若已存在则不新建
            stmt.executeUpdate("create database " + DATABASE_NAME);

        } catch (SQLException e) {
            throw e;
        }
    }

    private void createTables(Statement stmt) throws IOException, SQLException {
        String sqlTable = Files.forIO().readFrom(getClass().getResourceAsStream(sqlFilePath + "msg.sql"),
                "utf-8");
        // replace "topic"
        sqlTable = sqlTable.replaceAll(Pattern.quote("${topic}"), TOPIC);

        String[] tables = sqlTable.split(";");


        for (String table : tables) {
            if (table != null && table.trim().length() > 0) {
                stmt.execute(table.trim() + ";");
            }
        }
    }

    private Connection getConnection(String jdbcUrl) throws ClassNotFoundException, SQLException {
        Class.forName("com.mysql.jdbc.Driver");
        Connection conn = DriverManager.getConnection(jdbcUrl, MYSQL_USER, MYSQL_USER_PSW);

        return conn;
    }
}
