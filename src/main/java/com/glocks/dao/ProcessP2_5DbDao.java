package com.glocks.dao;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static com.glocks.EdrP3Process.appdbName;

public class ProcessP2_5DbDao {
    static Logger logger = LogManager.getLogger(ProcessP2_5DbDao.class);

    public static void insertIntoDbForP2_5(Connection conn, String csvFilePath, String operator) {

        String sdfTime = new SimpleDateFormat("yyyyMMdd").format(new Date());
        String tableName = appdbName + ".edr_" + sdfTime;
        var query = createTableQuery(tableName);
        try (PreparedStatement statement = conn.prepareStatement(query)) {
            statement.execute(query);
            logger.info(" MySQL table Created successfully.");
        } catch (SQLException e) {
            logger.error("Not able to create table : " + e.getMessage());
        }

        StringBuilder loadQuery = new StringBuilder();
        loadQuery.append("LOAD DATA LOCAL INFILE '")
                .append(csvFilePath)
                .append("' INTO TABLE ")
                .append(tableName)
                .append(" FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' " +
                        " IGNORE 1 LINES  (actual_imei,imsi, msisdn,edr_date_time, protocol , source, file_name, imei_arrival_time, operator_name  )  ");  //'\r\n'
        logger.info("tableName" + tableName + ";; Query :: " + loadQuery);

        try (PreparedStatement statement = conn.prepareStatement(loadQuery.toString())) {
            statement.execute(loadQuery.toString());
            logger.info("CSV file loaded into MySQL table successfully.");
        } catch (SQLException e) {
            logger.error(e + e.getMessage());
        }
    }

    private static String createTableQuery(String table) {
        return "CREATE TABLE IF NOT EXISTS  " + table + " " +
                " ( id bigint NOT NULL AUTO_INCREMENT, " +
                " edr_date_time timestamp DEFAULT NULL, " +
                " imei_arrival_time timestamp DEFAULT NULL, " +
                " created_on timestamp DEFAULT CURRENT_TIMESTAMP," +
                "  actual_imei varchar(20) DEFAULT NULL," +
                "  imsi varchar(20) DEFAULT NULL," +
                "  msisdn varchar(15) DEFAULT NULL," +
                "  operator_name varchar(50) DEFAULT NULL," +
                "  file_name varchar(250) DEFAULT NULL," +
                "  is_gsma_valid int DEFAULT 0," +
                "  is_duplicate int DEFAULT 0," +
                "  is_paired int DEFAULT 0," +
                "  is_invalid_imei int DEFAULT 0," +
                "  is_custom_paid int DEFAULT 0," +
                "  tac varchar(20) DEFAULT NULL," +
                "  device_type varchar(50) DEFAULT NULL," +
                "  source varchar(50) DEFAULT NULL," +
                "  protocol varchar(50) DEFAULT NULL," +
                "  UNIQUE (actual_imei,imsi)," +
                "  PRIMARY KEY (id)) ENGINE=InnoDB ";
    }
//        return "CREATE TABLE IF NOT EXISTS  " + table + "  (" +
//                "  id bigint NOT NULL AUTO_INCREMENT," +
//                "  edr_date_time timestamp NULL DEFAULT NULL," +
//                "  imei_arrival_time timestamp NULL DEFAULT NULL," +
//                "  created_on timestamp NULL DEFAULT CURRENT_TIMESTAMP," +
//                "  actual_imei varchar(20) DEFAULT NULL," +
//                "  imsi varchar(20) DEFAULT NULL," +
//                "  msisdn varchar(15) DEFAULT NULL," +
//                "  operator_name varchar(50) DEFAULT NULL," +
//                "  file_name varchar(250) DEFAULT NULL," +
//                "  is_gsma_valid int DEFAULT '0'," +
//                "  is_duplicate int DEFAULT '0'," +
//                "  is_paired int DEFAULT '0'," +
//                "  is_invalid_imei int DEFAULT '0'," +
//                "  is_custom_paid int DEFAULT '0'," +
//                "  tac varchar(20) DEFAULT NULL," +
//                "  device_type varchar(50) DEFAULT NULL," +
//                "  source varchar(50) DEFAULT NULL," +
//                "  protocol varchar(50) DEFAULT NULL," +
//                "  PRIMARY KEY (id) ) ";


}
//String csvFilePath = "/u02/ceirdata/processed_cdr/seatel/edr1/output/SEATEL_EDR1202403012009.csv";

//String query = "insert into  " + tableName + " ( imei,imsi, msisdn,timestamp, protocol , source, file_name,created_on) values "
//        + " (?, ?,?, ?,? ,?,?, " + dateFunction + ")";
//        try (
//PreparedStatement preparedStatement = conn.prepareStatement(query);) {
//        preparedStatement.setString(1, device_info.get("imei").toString());
//        preparedStatement.setString(2, device_info.get("imsi").toString());
//        preparedStatement.setString(3, device_info.get("msisdn").toString());
//        preparedStatement.setString(4, device_info.get("timestamp").toString());
//        preparedStatement.setString(5, device_info.get("protocol").toString());
//        preparedStatement.setString(6, device_info.get("source").toString());
//        preparedStatement.setString(7, device_info.get("file_name").toString());
//        //  logger.info("Query " + preparedStatement);
//        preparedStatement.execute();
////      logger.info("Inserted in " + tableName + " succesfully.");
//        } catch (SQLException e) {
//        logger.error("Error while executing " + e.getMessage(), e);
//        }

//PreparedStatement preparedStmt = null;
//        try (Connection connection = getConnection()){
//preparedStmt = connection.prepareStatement(loadQuery.toString());
//
//        connection.setAutoCommit(false);
//             preparedStmt.execute(loadQuery.toString());
//        connection.commit();PreparedStatement preparedStmt = null;
//        try (Connection connection = getConnection()){
//preparedStmt = connection.prepareStatement(loadQuery.toString());
//
//        connection.setAutoCommit(false);
//             preparedStmt.execute(loadQuery.toString());
//        connection.commit();