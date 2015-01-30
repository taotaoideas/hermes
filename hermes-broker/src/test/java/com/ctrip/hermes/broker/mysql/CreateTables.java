package com.ctrip.hermes.broker.mysql;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Pattern;

import org.junit.Test;
import org.unidal.helper.Files;

import com.site.lookup.util.StringUtils;

public class CreateTables {
    private final String DATABASE_NAME = "hermes";
    private String sqlFilePath = "resources/mysql/";
    private String TOPIC = "testtopic";

    private String m_path = "/data/appdatas/paas";

    private String m_datasourcePath = m_path + File.separator + "datasources.xml";

    private String m_jdbcUrl;

    private String m_user;

    private String m_password;

    @Test
    public void setupMysql() throws Exception {
        System.out.println("Starting to MySQL setup ...");

        validate();
        setupDatabase();
        setupConfigurationFiles();

        System.out.println("MySQL setup is finished!");
    }

    private void validate() throws Exception {
        m_jdbcUrl = readString("Please input jdbc url,default is[jdbc:mysql://127.0.0.1:3306]: ",
                "jdbc:mysql://127.0.0.1:3306");
        m_user = readString("Please input jdbc username[root]: ", "root");
        m_password = readString("Please input jdbc password: ", "");

        TOPIC = readString("Please input your topic(only lowercase): ", TOPIC);

        System.out.println("jdbc url : " + m_jdbcUrl);
        System.out.println("jdbc user : " + m_user);
        System.out.println("jdbc password : " + m_password);
    }

    private boolean setupDatabase() {
        Connection conn = null;
        Statement stmt = null;
        boolean isSuccess = false;

        try {
            System.out.println("Connecting to database(" + m_jdbcUrl + ") ...");
            conn = getConnection(m_jdbcUrl);
            System.out.println("Connected to database(" + m_jdbcUrl + ")");

            System.out.println("Creating database(paas) ...");
            stmt = conn.createStatement();
            createDatabase(stmt);
            System.out.println("Database(paas) is created successfully");

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
//            stmt.executeUpdate("drop database if exists " + DATABASE_NAME);
            stmt.executeUpdate("create database " + DATABASE_NAME);

        } catch (SQLException e) {
            throw e;
        }
    }

    private void createTables(Statement stmt) throws IOException, SQLException {
        String sqlTable = Files.forIO().readFrom(getClass().getResourceAsStream("setup_paas.sql"), "utf-8");
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
        Connection conn = DriverManager.getConnection(jdbcUrl, m_user, m_password);

        return conn;
    }

    private String readString(String prompt, String defaultString) {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        String str = null;

        try {
            System.out.print(prompt);
            str = br.readLine();

        } catch (IOException e) {
            e.printStackTrace();
        }

        if (StringUtils.isEmpty(str)) {
            return defaultString;
        } else {
            return str;
        }
    }

    private boolean setupConfigurationFiles() {
        System.out.println("Generating the configuration files to " + m_path + " ...");
        boolean isSuccess = false;

        try {
            System.out.println("Generating datasources.xml .");

            String datasources = Files.forIO().readFrom(getClass().getResourceAsStream("datasources.xml"), "utf-8");

            String url = null;

            if (m_jdbcUrl.endsWith("/")) {
                url = m_jdbcUrl + "paas";
            } else {
                url = m_jdbcUrl + "/paas";
            }

            datasources = datasources.replaceAll(Pattern.quote("${jdbc.url}"), url);
            datasources = datasources.replaceAll(Pattern.quote("${jdbc.user}"), m_user);
            datasources = datasources.replaceAll(Pattern.quote("${jdbc.password}"), m_password);

            Files.forIO().writeTo(new File(m_datasourcePath), datasources);

            System.out.println("Configuration files are generated successfully");

            isSuccess = true;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return isSuccess;
    }

}
