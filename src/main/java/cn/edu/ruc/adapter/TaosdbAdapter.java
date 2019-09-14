package cn.edu.ruc.adapter;

import java.sql.*;
import java.util.*;

import cn.edu.ruc.base.*;
import com.alibaba.druid.pool.DruidDataSource;
import com.taosdata.jdbc.TSDBDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ClassName TaosdbAdapter
 * @Description: TODO
 * @Author rainmaple
 * @Date 2019/9/14
 * @Version V1.0
 **/
public class TaosdbAdapter implements DBAdapter{
    private static final String TSDB_DRIVER = "com.taosdata.jdbc.TSDBDriver";
    private static final String JDBC_PROTOCAL = "jdbc:TAOS://";
    private static final String DB_NAME = "db";
    private String urlFormat = "%s%s:%d/%s?user=%s&password=%s";
    private String db_ip = "";
    private String db_user = "";
    private String db_password = "";
    private String db_port = "";
    private static final String ROOT_SERIES_NAME = "root.perform";
    private static final String TABLE_NAME = "temp";
    private String jdbcUrl = "";

    private Logger LOGGER = LoggerFactory.getLogger(getClass());
    private TsParamConfig tspc = null;
    static{
        try {
            Class.forName(TSDB_DRIVER);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Override
    public void initDataSource(TsDataSource ds, TsParamConfig tspc) {
        this.tspc = tspc;
        //初始化连接(数据库名不传参)
        db_ip =ds.getIp();
        db_port = ds.getPort();
        db_user = ds.getUser();
        db_password = ds.getPasswd();
        jdbcUrl = String.format(urlFormat, JDBC_PROTOCAL, db_ip, db_port,
                "",
                db_user, db_password);

        //初始化存储
        if (tspc.getTestMode().equals("write")) {
            initTimeseriesAndStorage(tspc);
        }
    }

    /**
     * //TODO 初始化时间序列，并设置storage
     *
     */
    private void initTimeseriesAndStorage(TsParamConfig tspc) {
        int deviceNum = tspc.getDeviceNum()*tspc.getWriteClients();
        int sensorNum = tspc.getSensorNum();
        deviceNum=150*500;
        Connection connection = null;
        Statement statement = null;
        try {
            //获取数据库连接
            connection = getConnection();
            statement = connection.createStatement();
            try {
                //创建库和表
                //TODO 超表设置？
                doCreateDbAndTable();
                /**
                String setStorageSql = "CREATE TABLE "+ROOT_SERIES_NAME+ "( timestamp TIMESTAMPTZ " +
                        " NOT NULL , device_id TEXT NOT NULL)";
                statement.executeUpdate(setStorageSql);

                String setHypertabelSql = "SELECT create_hypertable('"+ROOT_SERIES_NAME+"','timestamp','device_id',75000)";
                //"SELECT create_hypertable('conditions', 'time', 'location', 4);"
                statement.execute(setHypertabelSql);
                // 创建超表
                // 创建表结束**/
                try {
                    for(int sensorIdx=0;sensorIdx<sensorNum;sensorIdx++) {
                        String sensorCode="s_"+sensorIdx;
                        //ALTER TABLE conditions
                        //ADD COLUMN humidity DOUBLE PRECISION NULL;
                        String sql="ALTER TABLE "+ROOT_SERIES_NAME + " ADD COLUMN "
                                +sensorCode+"  DOUBLE PRECISION NULL;";
                        statement.addBatch(sql);
                    }
                    statement.executeBatch();
                    statement.clearBatch();
                    LOGGER.info("{} alter table finished[{}/{}].");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeStatement(statement);
            closeConnection(connection);
        }
    }

    @Override
    public Object preWrite(TsWrite tsWrite) {
        List<String> sql_sets = new ArrayList<>();
        LinkedList<TsPackage> pkgs = tsWrite.getPkgs();
        StringBuffer sqlBuffer = new StringBuffer();
        for (TsPackage pkg : pkgs) {
            StringBuffer valueStrBuffer = new StringBuffer();
            sqlBuffer.append("insert into"+" ");
            sqlBuffer.append(TABLE_NAME);
            sqlBuffer.append(".");
            String deviceCode = pkg.getDeviceCode();
            sqlBuffer.append(deviceCode);
            sqlBuffer.append("(");
            sqlBuffer.append("timestamp");
            Set<String> sensorCodes = pkg.getSensorCodes();
            valueStrBuffer.append("(");
            valueStrBuffer.append(pkg.getTimestamp());
            for (String sensorCode_enum : sensorCodes) {
                sqlBuffer.append(",");
                sqlBuffer.append(sensorCode_enum);
                valueStrBuffer.append(",");
                valueStrBuffer.append(pkg.getValue(sensorCode_enum));
            }
            sqlBuffer.append(") values");
            valueStrBuffer.append(")");
            sqlBuffer.append(valueStrBuffer);
            sql_sets.add(sqlBuffer.toString());
            sqlBuffer.setLength(0);
        }
        return sql_sets;
    }

    @Override
    public Status execWrite(Object write) {
        @SuppressWarnings("unchecked")
        List<String> sqls = (List<String>) write;
        Connection connection = getConnection();
        Statement statement = null;
        Long costTime = 0L;
        try {
            statement = connection.createStatement();
            for (String sql : sqls) {
                statement.addBatch(sql);
            }
            long startTime = System.nanoTime();
            statement.executeBatch();
            long endTime = System.nanoTime();
            costTime = endTime - startTime;
            statement.clearBatch();
        } catch (Exception e) {
            e.printStackTrace();
            return Status.FAILED(-1L);
        } finally {
            closeStatement(statement);
            closeConnection(connection);
        }
        return Status.OK(costTime);
    }

    @Override
    public Object preQuery(TsQuery tsQuery) {
        // TODO

        StringBuffer sc = new StringBuffer();
        sc.append("select ");
        switch (tsQuery.getQueryType()) {
            case 1://简单查询
                sc.append(tsQuery.getSensorName());
                sc.append(" ");
                break;
            case 2://分析查询
                sc.append("");
                if (tsQuery.getAggreType() == 1) {
                    sc.append("max_value(");
                }
                if (tsQuery.getAggreType() == 2) {
                    sc.append("min_value(");
                }
                if (tsQuery.getAggreType() == 3) {
                    sc.append("mean(");
                }
                if (tsQuery.getAggreType() == 4) {
                    sc.append("count(");
                }
                sc.append(tsQuery.getSensorName());
                sc.append(") ");
                break;
            case 3://分析查询
                sc.append("");
                if (tsQuery.getAggreType() == 1) {
                    sc.append("max_value(");
                }
                if (tsQuery.getAggreType() == 2) {
                    sc.append("min_value(");
                }
                if (tsQuery.getAggreType() == 3) {
                    sc.append("mean(");
                }
                if (tsQuery.getAggreType() == 4) {
                    sc.append("count(");
                }
                sc.append(tsQuery.getSensorName());
                sc.append(") ");
                break;
            default:
                break;
        }
        sc.append("from ");
        if (tsQuery.getQueryType() == 3) {
            String template = TABLE_NAME + ".%s";
            List<String> devices = tsQuery.getDevices();
            for (int index = 0; index < devices.size(); index++) {
                sc.append(String.format(template, devices.get(index)));
                if (index < (devices.size() - 1)) {
                    sc.append(",");
                } else {
                    sc.append(" ");
                }
            }
        } else {
            sc.append(TABLE_NAME);
            sc.append(".");
            sc.append(tsQuery.getDeviceName());
            sc.append(" ");
        }
        if (tsQuery.getStartTimestamp() != null) {
            sc.append("and ");
            sc.append("time >=");
            sc.append(tsQuery.getStartTimestamp());
            sc.append(" ");
        }
        if (tsQuery.getEndTimestamp() != null) {
            sc.append("and ");
            sc.append("time <=");
            sc.append(tsQuery.getEndTimestamp());
            sc.append(" ");
        }
        if (tsQuery.getSensorLtValue() != null) {
            sc.append("and ");
            sc.append(tsQuery.getSensorName());
            sc.append(">=");
            sc.append(tsQuery.getSensorLtValue());
            sc.append(" ");
        }
        if (tsQuery.getSensorGtValue() != null) {
            sc.append("and ");
            sc.append(tsQuery.getSensorName());
            sc.append("<=");
            sc.append(tsQuery.getSensorGtValue());
            sc.append(" ");
        }
        if (tsQuery.getGroupByUnit() != null && tsQuery.getQueryType() == 2) {
            sc.append("group by ");
            switch (tsQuery.getGroupByUnit()) {
                case 1:
                    sc.append(" (1s,[");
                    break;
                case 2:
                    sc.append(" (1m,[");
                    break;
                case 3:
                    sc.append(" (1h,[");
                    break;
                case 4:
                    sc.append(" (1d,[");
                    break;
                case 5:
                    sc.append(" (1M,[");
                    break;
                case 6:
                    sc.append(" (1y,[");
                    break;
                default:
                    break;
            }
            sc.append(tsQuery.getStartTimestamp());
            sc.append(",");
            sc.append(tsQuery.getEndTimestamp());
            sc.append("])");
        }
        return sc.toString().replaceFirst("and", "where");
    }

    @Override
    public Status execQuery(Object query) {
        Connection conn = getConnection();
        Statement statement = null;
        long costTime = 0;
        try {
            statement = conn.createStatement();
            long startTime = System.nanoTime();
            ResultSet rs = statement.executeQuery(query.toString());
            rs.next();
            long endTime = System.nanoTime();

            costTime = endTime - startTime;
        } catch (SQLException e) {
            e.printStackTrace();
            return Status.FAILED(-1);
        } finally {
            closeStatement(statement);
            closeConnection(conn);
        }
        return Status.OK(costTime, 1);
    }

    public static void main(String[] args) {
        DBAdapter adapter = new IotdbAdapter();
        TsQuery query = new TsQuery();
        query.setQueryType(1);
        query.setDeviceName("d_1");
        query.setSensorName("s_49");
        query.setAggreType(2);
        query.setStartTimestamp(System.currentTimeMillis() - 10000000L);
        query.setEndTimestamp(System.currentTimeMillis());
        query.setGroupByUnit(3);
        Object preQuery = adapter.preQuery(query);
        System.out.println(preQuery);
    }


    @Override
    public void closeAdapter() {
        // TODO Auto-generated method stub

    }

    private void doCreateDbAndTable() {
        LOGGER.info("\n---------------------------------------------------------------");
        LOGGER.info("Start creating databases and tables...");
        String sql = "";
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()){

            sql = "create database if not exists " + DB_NAME;
            stmt.executeUpdate(sql);
            LOGGER.info("Successfully executed: %s\n", sql);
            LOGGER.info("{} create database finished[{}/{}]");

            sql = "use " + DB_NAME;
            stmt.executeUpdate(sql);
            LOGGER.info("Successfully executed: %s\n", sql);
            LOGGER.info("{} use database"+DB_NAME+" finished[{}/{}]");

            sql = "create table if not exists " + TABLE_NAME + " (ts timestamp, v1 int) tags(t1 int)";
            stmt.executeUpdate(sql);
            LOGGER.info("Successfully executed: %s\n", sql);
            LOGGER.info("{} create table finished[{}/{}]");
/*
            for (int i = 0; i < this.tablesCount; i++) {
                sql = String.format("create table if not exists %s%d using %s tags(%d)", this.tablePrefix, i,
                        this.metricsName, i);
                stmt.executeUpdate(sql);
                LOGGER.info("Successfully executed: %s\n", sql);
            }*/
        } catch (SQLException e) {
            e.printStackTrace();
            LOGGER.info("Failed to execute SQL: %s\n", sql);
            System.exit(4);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(4);
        }
        System.out.println("Successfully created databases and tables");
    }

    private Connection getConnection() {
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(jdbcUrl);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connection;
    }

    private void closeConnection(Connection conn) {
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void closeStatement(Statement statement) {
        try {
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
