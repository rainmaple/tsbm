package cn.edu.ruc.adapter;

import cn.edu.ruc.base.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.taosdata.jdbc.TSDBDriver;

import java.sql.*;
import java.util.List;
import java.util.Properties;

/**
 * @ClassName TaosdbAdapter
 * @Destrbription: 涛思数据库Adapter
 * @Author rainmaple
 * @Date 2019/9/14 
 * @Version V1.0
 **/
public class TaosdbAdapter implements DBAdapter{
    private  String DRIVER_CLASS ="cn.edu.tsinghua.iotdb.jdbc.TsfileDriver";
    private  String URL ="jdbc:TAOS://%s:%s/";
    private  String USER ="";
    private  String PASSWD ="";
    private static final String ROOT_SERIES_NAME="root.perform";
    private Logger logger= LoggerFactory.getLogger(getClass());
    private TsParamConfig tspc=null;

    @Override
    public void initDataSource(TsDataSource ds, TsParamConfig tspc) {
        this.tspc=tspc;
        //初始化连接
        try {
            Class.forName(DRIVER_CLASS);
        } catch (Exception e) {
            e.printStackTrace();
        }
        URL=String.format(URL,ds.getIp(),ds.getPort());
        USER=ds.getUser();
        PASSWD=ds.getPasswd();
        //初始化数据库
        //getDataSource();
        //初始化存储组
        //初始化存储组
        if(tspc.getTestMode().equals("write")) {
            initTimeseriesAndStorage(tspc);
        }
    }

    @Override
    public Object preWrite(TsWrite tsWrite) {
        return null;
    }

    @Override
    public Status execWrite(Object write) {
        return null;
    }

    @Override
    public Object preQuery(TsQuery tsQuery) {
        StringBuffer strb=new StringBuffer();
        strb.append("select ");
        int type = tsQuery.getQueryType();
        switch (type) {
            case 1://简单查询
                strb.append(tsQuery.getSensorName());
                strb.append(" ");
                break;
            case 2://分析查询
                strb.append("");
                if(tsQuery.getAggreType()==1) {
                    strb.append("max(");
                }
                if(tsQuery.getAggreType()==2) {
                    strb.append("min(");
                }
                if(tsQuery.getAggreType()==3) {
                    strb.append("avg(");
                }
                if(tsQuery.getAggreType()==4) {
                    strb.append("count(");
                }
                strb.append(tsQuery.getSensorName());
                strb.append(") ");
                break;
            case 3://分析查询
                strb.append("");
                if(tsQuery.getAggreType()==1) {
                    strb.append("max(");
                }
                if(tsQuery.getAggreType()==2) {
                    strb.append("min(");
                }
                if(tsQuery.getAggreType()==3) {
                    strb.append("avg(");
                }
                if(tsQuery.getAggreType()==4) {
                    strb.append("count(");
                }
                strb.append(tsQuery.getSensorName());
                strb.append(") ");
                break;
            default:
                break;
        }
        strb.append("from ");
        if(tsQuery.getQueryType()==3){
            strb.append(ROOT_SERIES_NAME+" where device_id in (");
            List<String> devices = tsQuery.getDevices();
            for(int index=0;index<devices.size();index++){
                strb.append("'"+devices.get(index)+"'");
                if(index<(devices.size()-1)){
                    strb.append(",");
                }else{
                    strb.append(" ");
                }
            }
            strb.append(")");
        }else{
            strb.append(ROOT_SERIES_NAME);
            strb.append(" where device_id in (");
            strb.append("'"+tsQuery.getDeviceName()+"'");
            strb.append(")");
        }
        if(tsQuery.getStartTimestamp()!=null) {
            strb.append(" and ");
            strb.append("timestamp >=");
            strb.append("to_timestamp(" +tsQuery.getStartTimestamp() + ")");
            strb.append(" ");
        }
        if(tsQuery.getEndTimestamp()!=null) {
            strb.append(" and ");
            strb.append("timestamp <=");
            strb.append("to_timestamp("+tsQuery.getEndTimestamp()+")");
            strb.append(" ");
        }
        if(tsQuery.getSensorLtValue()!=null) {
            strb.append(" and ");
            strb.append(tsQuery.getSensorName());
            strb.append(">=");
            strb.append(tsQuery.getSensorLtValue());
            strb.append(" ");
        }
        if(tsQuery.getSensorGtValue()!=null) {
            strb.append(" and ");
            strb.append(tsQuery.getSensorName());
            strb.append("<=");
            strb.append(tsQuery.getSensorGtValue());
            strb.append(" ");
        }
        if(tsQuery.getGroupByUnit()!=null&&tsQuery.getQueryType()==2) {
            strb.append(" group by ");
            switch (tsQuery.getGroupByUnit()) {
                case 1:
                    strb.append(" time_bucket('1 second', timestamp) ");
                    break;
                case 2:
                    //time_bucket('5 minutes', time)
                    strb.append(" time_bucket('1 minute', timestamp)");
                    break;
                case 3:
                    strb.append(" time_bucket('1 hour', timestamp)");
                    break;
                case 4:
                    strb.append(" time_bucket('1 day', timestamp)");
                    break;
                case 5:
                    strb.append(" time_bucket('1 month', timestamp)");
                    break;
                case 6:
                    strb.append(" time_bucket('1 year', timestamp)");
                    break;
                default:
                    break;
            }
        }
        logger.info(strb.toString());
        return strb.toString();
    }

    @Override
    public Status execQuery(Object query) {
        Connection conn= null;
        try {
            conn = getConn();
        } catch (Exception e) {
            e.printStackTrace();
        }
        Statement statement=null;
        long costTime=0;
        try {
            statement=conn.createStatement();
            long startTime=System.nanoTime();
//			System.out.println(query);
            ResultSet rs = statement.executeQuery(query.toString());
            rs.next();
            long endTime=System.nanoTime();
//			if(rs.next()){
//				System.out.println(rs.getObject(1));
//			}
            costTime=endTime-startTime;
//			System.out.println(query.toString()+"============"+costTime/1000+" us=============");
        } catch (SQLException e) {
            e.printStackTrace();
            return Status.FAILED(-1);
        }finally{
            closeStatement(statement);
            closeConnection(conn);
        }
        return Status.OK(costTime, 1);
    }

    @Override
    public void closeAdapter() {

    }
    private void initTimeseriesAndStorage(TsParamConfig tspc) {
        int deviceNum = tspc.getDeviceNum()*tspc.getWriteClients();
        int sensorNum = tspc.getSensorNum();
        deviceNum=150*500;
        Connection connection = null;
        Statement statement = null;
        try {
            connection = getConn();
            statement = connection.createStatement();
            try {
                //创建表
                String setStorageSql="CREATE TABLE "+ROOT_SERIES_NAME+ "( timestamp TIMESTAMPTZ NOT NULL , " +
                        "device_id TEXT NOT NULL)";
                statement.executeUpdate(setStorageSql);
                logger.info("{} create table finished[{}/{}]");

                // 创建超表
                // 创建表结束
                try {
                    for(int sensorIdx=0;sensorIdx<sensorNum;sensorIdx++) {
                        String sensorCode="s_"+sensorIdx;
                        //ALTER TABLE conditions
                        //ADD COLUMN humidity DOUBLE PRECISION NULL;
                        String sql="ALTER TABLE "+ROOT_SERIES_NAME
                                + " ADD COLUMN " +sensorCode+"DOUBLE PRECISION NULL;";
                        statement.addBatch(sql);
                    }
                    statement.executeBatch();
                    statement.clearBatch();
                    logger.info("{} alter table finished[{}/{}].");
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

    public Connection getConn() throws Exception{
        Class.forName("com.taosdata.jdbc.TSDBDriver");
        String jdbcUrl = "jdbc:TAOS://127.0.0.1:0/db?user=root&password=taosdata";
        Properties connProps = new Properties();
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_USER, "root");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, "taosdata");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_CONFIG_DIR, "/etc/taos");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        Connection conn = DriverManager.getConnection(jdbcUrl, connProps);
        return conn;
    }

    private void closeConnection(Connection conn){
        try {
            if(conn!=null){
                conn.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private void closeStatement(Statement statement){
        try {
            if(statement!=null){
                statement.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
