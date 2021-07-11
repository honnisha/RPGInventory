package ru.endlesscode.rpginventory.mysql;

import ru.endlesscode.rpginventory.RPGInventory;
import ru.endlesscode.rpginventory.utils.Log;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class MysqlConn {
    private static final String DB_URL = "jdbc:mysql://{0}:{1}/{2}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
    private static final String Table_SQL = "CREATE TABLE `{0}` ( `id` INT NOT NULL AUTO_INCREMENT COMMENT 'Auto increment' , `uuid` VARCHAR(38) NOT NULL COMMENT 'User UUID' , `data` VARBINARY(65000) NOT NULL COMMENT 'User data' , `crafttime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Create date' , `updatetime` TIMESTAMP on update CURRENT_TIMESTAMP NULL COMMENT 'Update time' , PRIMARY KEY (`id`)) ENGINE = MyISAM COMMENT = '{1}';";
    private static final String SELECT_SQL = "select `id` FROM {0} WHERE uuid='{1}'";
    private static final String INSERT_SQL = "INSERT INTO `{0}`(`uuid`, `data`) VALUES ('{1}', '{2}')";
    private static final String UPDATE_SQL = "UPDATE `{0}` SET `data`='{1}' WHERE uuid='{2}'";
    private static final String GET_SQL = "select `data` FROM {0} WHERE uuid='{1}'";
    private static final String GET_ALL_SQL = "select `uuid`,`data` FROM `{0}`";
    private static final String DELETE_SQL = "DELETE FROM `{0}` WHERE uuid='{1}'";

    public static final String TableInventory = "inventory";
    public static final String TableBackpack = "backpack";

    public static final String CommentInventory = "Inventory table";
    public static final String CommentBackpack = "Backpack table";

    private static Connection conn = null;
    private static final Object lock = new Object();

    private static boolean connect() {
        try {
            if (conn != null)
                conn.close();
            RPGInventory.getInstance().getLogger().info("MySQL connecting.");
            String USER = RPGInventory.getInstance().getConfig().getString("mysql.user", "root");
            String PASS = RPGInventory.getInstance().getConfig().getString("mysql.password", "123456");
            String IP = RPGInventory.getInstance().getConfig().getString("mysql.ip", "127.0.0.1");
            String PORT = RPGInventory.getInstance().getConfig().getString("mysql.port", "3306");
            String DATABASE = RPGInventory.getInstance().getConfig().getString("mysql.database", "3306");

            conn = DriverManager.getConnection(DB_URL.replace("{0}", IP).replace("{1}", PORT).replace("{2}", DATABASE), USER, PASS);
            Log.i(String.format("Connected to mysql %s:%s database: %s user: %s", IP, PORT, DATABASE, USER));
            return true;
        } catch (Exception e) {
            RPGInventory.getInstance().closeMysql();
            e.printStackTrace();
        }
        return false;
    }

    public static void start() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            if (connect()) {
                Log.i("MySQL connection succeeded.");
                checkTable();
            } else {
                Log.w("MySQL connection failed.");
                RPGInventory.getInstance().closeMysql();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void stop() {
        try {
            if (conn != null)
                conn.close();
        } catch (Exception e) {
            RPGInventory.getInstance().getLogger().warning("Failed to close MySQL connection.");
            RPGInventory.getInstance().closeMysql();
            e.printStackTrace();
        }
    }

    private static void checkTable() {
        String sql = String.format("show tables like '%s';", TableInventory);
        try {
            Statement stmt = conn.createStatement();
            ResultSet resultSet = stmt.executeQuery(sql);
            if (!resultSet.isBeforeFirst()) {
                createTable(TableInventory, CommentInventory);
            } else {
                resultSet.close();
            }
            sql = "show tables like '" + TableBackpack + "';";
            resultSet = stmt.executeQuery(sql);
            if (!resultSet.isBeforeFirst()) {
                createTable(TableBackpack, CommentBackpack);
            }
            resultSet.close();
            stmt.close();
        } catch (Exception e) {
            if (reconnect()) {
                checkTable();
            }
            Log.w("Failed to check MySQL table.");
            Log.w(String.format("sql: %s", sql));
            e.printStackTrace();
        }
    }

    private static boolean reconnect() {
        synchronized (lock) {
            try {
                if (conn == null || conn.isClosed() || conn.isReadOnly()) {
                    Log.i("Reconnected.");
                    return connect();
                }
            } catch (Exception e) {
                Log.w("Failed to reconnect MySQL.");
                RPGInventory.getInstance().closeMysql();
                e.printStackTrace();
                return false;
            }
            return true;
        }
    }

    private static void createTable(String name, String comment) {
        try {
            Statement stmt = conn.createStatement();
            String sql = Table_SQL.replace("{0}", name).replace("{1}", comment);
            stmt.execute(sql);
            stmt.close();
            Log.i(String.format("Table %s created; comment: %s", name, comment));
        } catch (Exception e) {
            if (reconnect()) {
                createTable(name, comment);
            }
            Log.w("Failed to create MySQL table.");
            e.printStackTrace();
        }
    }

    public static boolean checkNull(String table, String uuid) {
        boolean data = false;
        String sql = SELECT_SQL.replace("{0}", table).replace("{1}", uuid);
        try {
            Statement stmt = conn.createStatement();
            ResultSet resultSet = stmt.executeQuery(sql);
            if (!resultSet.isBeforeFirst()) {
                data = true;
            }
            resultSet.close();
            stmt.close();
        } catch (Exception e) {
            if (reconnect()) {
                return checkNull(table, uuid);
            }
            Log.w(String.format("Failed to check player data in MySQL from table %s.", table));
            Log.w(String.format("sql: %s", sql));
            e.printStackTrace();
        }
        return data;
    }

    public static void setData(String table, String uuid, String data) {
        String sql = INSERT_SQL.replace("{0}", table).replace("{1}", uuid).replace("{2}", data);
        try {
            Statement stmt = conn.createStatement();
            stmt.execute(sql);
            stmt.close();
        } catch (Exception e) {
            if (reconnect()) {
                setData(table, uuid, data);
            }
            Log.w(String.format("Failed to insert player data in MySQL; table: %s; uuid: %s; data: %s", table, uuid, data));
            Log.w(String.format("sql: %s", sql));
            e.printStackTrace();
        }
    }

    public static void update(String table, String uuid, String data) {
        try {
            String sql = UPDATE_SQL.replace("{0}", table).replace("{1}", data).replace("{2}", uuid);
            Statement stmt = conn.createStatement();
            stmt.execute(sql);
            stmt.close();
        } catch (Exception e) {
            if (reconnect()) {
                update(table, uuid, data);
            }
            Log.w(String.format("Failed to update player data in MySQL; table: %s; uuid: %s; data: %s", table, uuid, data));
            e.printStackTrace();
        }
    }

    public static String getData(String table, String uuid) {
        try {
            String sql = GET_SQL.replace("{0}", table)
                    .replace("{1}", uuid);
            Statement stmt = conn.createStatement();
            ResultSet resultSet = stmt.executeQuery(sql);
            resultSet.next();
            String data = resultSet.getString("data");
            resultSet.close();
            stmt.close();
            return data;
        } catch (Exception e) {
            if (reconnect()) {
                return getData(table, uuid);
            }
            Log.w(String.format("Failed to get player data in MySQL; table: %s; uuid: %s", table, uuid));
            e.printStackTrace();
        }
        return null;
    }

    public static Map<String, String> getAllData(String table) {
        try {
            Map<String, String> list = new HashMap<>();
            String sql = GET_ALL_SQL.replace("{0}", table);
            Statement stmt = conn.createStatement();
            ResultSet resultSet = stmt.executeQuery(sql);
            while (resultSet.next()) {
                String uuid = resultSet.getString("uuid");
                String data = resultSet.getString("data");
                list.put(uuid, data);
            }
            resultSet.close();
            stmt.close();
            return list;
        } catch (Exception e) {
            if (reconnect()) {
                return getAllData(table);
            }
            Log.w(String.format("Failed to get all player data in MySQL; table: %s", table));
            e.printStackTrace();
        }
        return null;
    }

    public static void delete(String table, String uuid) {
        try {
            String sql = DELETE_SQL.replace("{0}", table)
                    .replace("{1}", uuid);
            Statement stmt = conn.createStatement();
            stmt.execute(sql);
            stmt.close();
        } catch (Exception e) {
            if (reconnect()) {
                delete(table, uuid);
            }
            Log.w(String.format("Failed to delete player data in MySQL; table: %s", table));
            e.printStackTrace();
        }
    }
}