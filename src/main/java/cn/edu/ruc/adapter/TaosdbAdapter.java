package cn.edu.ruc.adapter;

import java.sql.*;
import java.util.*;

import cn.edu.ruc.base.*;
import com.taosdata.jdbc.TSDBDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ClassName TaosdbAdapter
 * @Description: 涛思数据库适配器
 * @Author rainmaple
 * @Date 2019/9/14
 * @Version V1.0
 **/
public class TaosdbAdapter implements DBAdapter {
    /**
     * @Description: TSDB_DRIVER = "com.taosdata.jdbc.TSDBDriver";   //驱动
     * JDBC_PROTOCAL = "jdbc:TAOS://";                 //连接协议
     * String DB_NAME = "db";                          //数据库名
     * ROOT_SERIES_NAME = "root.perform";              //超级表
     * TABLE_NAME = "temp";                            //表名
     **/
    private static final String TSDB_DRIVER = "com.taosdata.jdbc.TSDBDriver";
    private static final String JDBC_PROTOCAL = "jdbc:TAOS://";
    private static final String DB_NAME = "db";
    private static final String TABLE_NAME = "conditions";
    private static final String URL_Format = "%s%s:%s/%s?user=%s&password=%s";

    private String db_ip = "";
    private String db_user = "";
    private String db_password = "";
    private String db_port = "";

    private String jdbcUrl = "";

    private Logger LOGGER = LoggerFactory.getLogger(getClass());

    private TsParamConfig tspc = null;

    static {
        try {
            Class.forName(TSDB_DRIVER);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * //TODO 初始化数据源，设置SQL源和初始化存储
     */
    @Override
    public void initDataSource(TsDataSource ds, TsParamConfig tspc) {
        this.tspc = tspc;
        //初始化连接(数据库名不传参)
        db_ip = ds.getIp();
        db_port = ds.getPort();
        db_user = ds.getUser();
        db_password = ds.getPasswd();
        jdbcUrl = String.format(URL_Format, JDBC_PROTOCAL, db_ip, db_port,
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
     * @param tspc
     */
    private void initTimeseriesAndStorage(TsParamConfig tspc) {
        int deviceNum = tspc.getDeviceNum() * tspc.getWriteClients();
        int sensorNum = tspc.getSensorNum();
        deviceNum = 150 * 500;
        Connection connection = null;
        Statement statement = null;
        try {
            //获取数据库连接
            connection = getConnection();
            statement = connection.createStatement();
            //创建库和表
            doCreateDbAndTable(deviceNum, sensorNum);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeStatement(statement);
            closeConnection(connection);
        }
    }

    /****
     * @Method preWrite 写入数据预处理
     * 对pkgs里的数据包建立批量sql
     * 时间戳 YYYY-MM-DD HH:mm:ss.MS
     * @param tsWrite
     * @return
     */
    @Override
    public Object preWrite(TsWrite tsWrite) {
        List<String> sqls_list = new ArrayList<>();
        LinkedList<TsPackage> pkgs = tsWrite.getPkgs();
        StringBuffer sqlBufferSets = new StringBuffer();
        for (TsPackage pkg : pkgs) {
            StringBuffer valueBuffer = new StringBuffer();
            sqlBufferSets.append("INSERT INTO" + " ");
            sqlBufferSets.append(TABLE_NAME);
            sqlBufferSets.append("(");
            sqlBufferSets.append("timestamp , device_id");

            Set<String> sensorCodes = pkg.getSensorCodes();
            String deviceCode = pkg.getDeviceCode();
            valueBuffer.append("(");
            // pkg.getTimestamp()  now时间改为这个时间
            valueBuffer.append(""+ pkg.getTimestamp() +"");
            valueBuffer.append(",");
            valueBuffer.append("'" + deviceCode + "'");

            for (String sensorCode : sensorCodes) {
                sqlBufferSets.append(",");
                sqlBufferSets.append(sensorCode);

                valueBuffer.append(",");
                String value_sensorCode = pkg.getValue(sensorCode).toString();
                valueBuffer.append(value_sensorCode);
            }

            sqlBufferSets.append(") VALUES");
            valueBuffer.append(")");
            sqlBufferSets.append(valueBuffer);
            sqls_list.add(sqlBufferSets.toString());
            sqlBufferSets.setLength(0);
        }
        return sqls_list;
    }

    /****
     * @Method execWrite 正式写入数据
     * 对预处理中sql集合进行批量执行
     * @param write
     * @return
     */
    @Override
    public Status execWrite(Object write) {
        List<String> sqls_list = (List<String>) write;
        Connection connection = null;
        Statement statement = null;
        Long costTime = 0L;
        try {
            connection = this.getConnection();
            statement = connection.createStatement();
            for (String sql_record : sqls_list) {
                statement.addBatch(sql_record);
            }
            long startTime = System.nanoTime();
            statement.executeBatch();
            long endTime = System.nanoTime();
            costTime = endTime - startTime;
            //清除batch sqls
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

    /***
     * @Method preQuery 传入查询，进行解析规格化成sql
     * @decription
     *  查询涉及具体字段
     *  private static final long serialVersionUID = 1L;
     * 	private int queryType=1;//查询类型 1:简单查询 2:分析查询  3:多设备聚合
     * 	private String deviceName;//设备名称 必传
     * 	private String sensorName;//传感器名称  必传
     * 	private Long startTimestamp;
     * 	private Long endTimestamp;
     * 	private Double sensorLtValue;//传感器值小于某个值
     * 	private Double sensorGtValue;//传感器值大于某个值
     * 	private Integer groupByUnit;//聚合查询 组类别 1:s 2:min 3:hour 4:day 5:month 6:year
     * 	private Integer aggreType;//聚合查询类型 1 max 2 min 3 avg 4 count
     *
     * @param tsQuery
     * @return String
     */
    @Override
    public Object preQuery(TsQuery tsQuery) {
        StringBuffer sc = new StringBuffer();
        sc.append("SELECT" + " ");
        switch (tsQuery.getQueryType()) {
            case 1://简单查询
                sc.append(tsQuery.getSensorName());
                sc.append(" ");
                break;
            case 2://分析查询
                if (tsQuery.getAggreType() == 1) {
                    sc.append("MAX(");
                }
                if (tsQuery.getAggreType() == 2) {
                    sc.append("MIN(");
                }
                if (tsQuery.getAggreType() == 3) {
                    sc.append("AVG(");
                }
                if (tsQuery.getAggreType() == 4) {
                    sc.append("COUNT(");
                }
                sc.append(tsQuery.getSensorName());
                sc.append(")" + " ");
                break;
            case 3://分析查询
                if (tsQuery.getAggreType() == 1) {
                    sc.append("MAX(");
                }
                if (tsQuery.getAggreType() == 2) {
                    sc.append("MIN(");
                }
                if (tsQuery.getAggreType() == 3) {
                    sc.append("AVG(");
                }
                if (tsQuery.getAggreType() == 4) {
                    sc.append("COUNT(");
                }
                sc.append(tsQuery.getSensorName());
                sc.append(")" + " ");
                break;
            default:
                break;
        }

        sc.append("FROM" + " ");

        if (tsQuery.getQueryType() == 3) {
            String template = TABLE_NAME + ".%s";
            List<String> devices = tsQuery.getDevices();
            for (int index = 0; index < devices.size(); index++) {
                String tableName = String.format(template, devices.get(index));
                sc.append(tableName);
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
            sc.append("AND" + " ");
            sc.append("ts >=");
            sc.append(tsQuery.getStartTimestamp());
            sc.append(" ");
        }
        if (tsQuery.getEndTimestamp() != null) {
            sc.append("AND" + " ");
            sc.append("ts <=");
            sc.append(tsQuery.getEndTimestamp());
            sc.append(" ");
        }
        if (tsQuery.getSensorLtValue() != null) {
            sc.append("AND" + " ");
            sc.append(tsQuery.getSensorName());
            sc.append(">=");
            sc.append(tsQuery.getSensorLtValue());
            sc.append(" ");
        }
        if (tsQuery.getSensorGtValue() != null) {
            sc.append("AND" + " ");
            sc.append(tsQuery.getSensorName());
            sc.append("<=");
            sc.append(tsQuery.getSensorGtValue());
            sc.append(" ");
        }
        if (tsQuery.getGroupByUnit() != null && tsQuery.getQueryType() == 2) {
            //sc.append("GROUP BY" + " ");
            sc.append("INTERVAL" + " ");
            switch (tsQuery.getGroupByUnit()) {
                case 1:
                    sc.append(" " + "(1s");
                    break;
                case 2:
                    sc.append(" " + "(1m");
                    break;
                case 3:
                    sc.append(" " + "(1h");
                    break;
                case 4:
                    sc.append(" " + "(1d");
                    break;
                case 5:
                    sc.append(" " + "(1M");
                    break;
                case 6:
                    sc.append(" " + "(1y");
                    break;
                default:
                    break;
            }
            //sc.append(tsQuery.getStartTimestamp());
            //sc.append(",");
            //sc.append(tsQuery.getEndTimestamp());
            sc.append(")");
        }
        return sc.toString().replaceFirst("AND", "WHERE");
    }

    /***
     * @Method 执行查询预处理后的sql 并计算耗时
     * @decription
     * @param  query
     * @return com.edu.ruc.base.Status
     */
    @Override
    public Status execQuery(Object query) {
        Connection conn = null;
        Statement statement = null;
        long costTime = 0;
        try {
            conn = this.getConnection();
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
        TsQuery query = new TsQuery();
        query.setAggreType(2);
        query.setDeviceName("d2");
        query.setSensorName("s1");
        query.setSensorLtValue(54.0);
        query.setSensorGtValue(12.0);
        query.setAggreType(2);
        query.setGroupByUnit(2);
        query.setQueryType(1);
        DBAdapter adapter = new TaosdbAdapter();
        System.out.println(adapter.preQuery(query));
    }


    @Override
    public void closeAdapter() {
        // TODO Auto-generated method stub

    }

    /***
     * 创建表
     * @schema ts timestamp , device_id TEXT ,sensorCode DOUBLE...sensorCode DOUBLE
     *
     */

    private void doCreateDbAndTable(int deviceName, int sensorNum) {
        LOGGER.info("\n---------------------------------------------------------------");
        LOGGER.info("Start creating databases and tables...");
        String sql = "";
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {

            sql = "create database if not exists " + DB_NAME;
            stmt.executeUpdate(sql);
            LOGGER.info("Successfully executed: %s\n", sql);
            LOGGER.info("{} create database finished[{}/{}]");

            sql = "use " + DB_NAME;
            stmt.executeUpdate(sql);
            LOGGER.info("Successfully executed: %s\n", sql);
            LOGGER.info("{} use database" + DB_NAME + " finished[{}/{}]");

            sql = "create table if not exists " + TABLE_NAME + " (ts timestamp NOT NULL, device_id binary(50) NOT NULL);";
            stmt.executeUpdate(sql);
            //批量修改表字段
            for (int sensorIdx = 0; sensorIdx < sensorNum; sensorIdx++) {
                String sensorCode = "s_" + sensorIdx;
                String sql_alter = "ALTER TABLE " + TABLE_NAME + " ADD COLUMN "
                        + sensorCode + "  DOUBLE ;";
                stmt.addBatch(sql_alter);
            }
            stmt.executeBatch();
            stmt.clearBatch();
            LOGGER.info("{} alter table finished[{}/{}].");

            LOGGER.info("Successfully executed: %s\n", sql);
            LOGGER.info("{} create table finished[{}/{}]");

        } catch (SQLException e) {
            e.printStackTrace();
            LOGGER.info("Failed to execute SQL: %s\n", sql);
            System.exit(4);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(4);
        }
        LOGGER.info("{}Successfully created databases and tables");
    }

    private Connection getConnection() {
        Connection connection = null;
        try {
            Properties connProps = new Properties();
            connProps.setProperty(TSDBDriver.PROPERTY_KEY_CONFIG_DIR, "/etc/taos");
            connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
            connProps.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
            connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
            connection = DriverManager.getConnection(jdbcUrl, connProps);
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
