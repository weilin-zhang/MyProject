package cn.sibat.kgj;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.HashMap;

/**
 * 客管局越轨数据预警工具
 * Created by wing1995 on 2017/8/8.
 */
public class CleanBolt {

    static HashMap<String,String> busid = new HashMap<>(); //存放车牌号对应公司名称的散列表
    static HashMap<String,String> buscompany = new HashMap<>(); //存放公司名称对应车辆所属辖区局的散列表
    static HashMap<String,String> busrange = new HashMap<>(); //存放车牌号对应车辆类型的散列表
    static Connection conn = null;

    /**
     * 构造器，初始化生成HashMap
     */
    public CleanBolt() {
        getAll();
    }

    /**
     * 连接数据库
     * @return Connection
     */
    public Connection getconn(){
        String diver = "org.postgresql.Driver";
        String url = "jdbc:postgresql://172.16.100.3:5432/kgj";
        String username = "kgj";
        String passwd = "kgj123";
        try{
            Class.forName(diver);//加载对应驱动
            conn = DriverManager.getConnection(url, username, passwd);
        }catch(ClassNotFoundException | SQLException e){
            e.printStackTrace();
        }
        return conn;
    }

    /**
     * 获取动态表和静态表中数据，并将数据存储到散列表中
     * @return null
     */
    public Integer getAll() {
        if(busid.isEmpty()){ //初始化
            Connection conn = getconn();
            String sql1 = "select vhcl_no from t_charter_bus";
            String sql2 = "select comp_name from t_charter_bus";
            String sql3 = "select vhcl_type from t_charter_bus";
            String sql4 = "select comp_name from t_charter_bus_company";
            String sql5 = "select register_address_id from t_charter_bus_company";
            String sql6 = "select vhcl_no from t_charter_bus";
            PreparedStatement pstmt1;
            PreparedStatement pstmt2;
            PreparedStatement pstmt3;
            PreparedStatement pstmt4;
            PreparedStatement pstmt5;
            PreparedStatement pstmt6;

            try {
                pstmt1 = conn.prepareStatement(sql1); //预编译SQL语句
                pstmt2 = conn.prepareStatement(sql2);
                pstmt3 = conn.prepareStatement(sql3);
                pstmt4 = conn.prepareStatement(sql4);
                pstmt5 = conn.prepareStatement(sql5);
                pstmt6 = conn.prepareStatement(sql6);
                ResultSet rs1 = pstmt1.executeQuery(); //使用预编译SQL语句执行查询，返回的ResuktSet拥有指针，可以指向数据的当前行
                ResultSet rs2 = pstmt2.executeQuery();
                ResultSet rs3 = pstmt3.executeQuery();
                ResultSet rs4 = pstmt4.executeQuery();
                ResultSet rs5 = pstmt5.executeQuery();
                ResultSet rs6 = pstmt6.executeQuery();

                //获取对应列的所有行数据，并将数据分别放到不同的HashMap中
                while (rs1.next() && rs2.next()) {
                    String keyid = rs1.getString(1);
                    String value = rs2.getString(1);
                    busid.put(keyid, value);
                }
                while (rs6.next() && rs3.next()) {
                    String keyid = rs6.getString(1);
                    String value = rs3.getString(1);
                    busrange.put(keyid, value);
                }
                while (rs4.next() && rs5.next()) {
                    String keyid = rs4.getString(1);
                    String value = rs5.getString(1);
                    buscompany.put(keyid, value);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    /**
     * 将新生成的报警数据插入t_gps_warning表中
     * @param line 每一行记录
     * @return 插入的行数
     */
    public int insert(Line line){
        Connection conn = getconn();
        int i = 0;
        String sql = "insert into t_gps_warning (warningtime,warningtime_end,bus_code,bus_id,longitude,latitude,longitude_end,latitude_end,color,comp_name,register_address_id,speed,warningtype) values(?,?,?,?,?,?,?,?,?,?,?,?,?)";
        PreparedStatement pstmt;
        try{
            pstmt = conn.prepareStatement(sql);
            pstmt.setTimestamp(1, line.warningTimeStart);
            pstmt.setTimestamp(2, line.warningTimeEnd);
            pstmt.setString(3, line.id);
            pstmt.setString(4, line.device);
            pstmt.setDouble(5, line.lonStart);
            pstmt.setDouble(6, line.latStart);
            pstmt.setDouble(7, line.lonEnd);
            pstmt.setDouble(8, line.latEnd);
            pstmt.setString(9, line.color);
            pstmt.setString(10, line.company);
            pstmt.setString(11, line.area);
            pstmt.setInt(12, line.speed);
            pstmt.setInt(13, line.type);
            i = pstmt.executeUpdate(); //返回SQL语句插入的行数
            pstmt.close();
            conn.close();
        }catch(SQLException e){
            e.printStackTrace();
        }
        return i;
    }

    /**
     * 更改t_gps_warning表中数据
     * @param line 记录
     * @return i 成功更改表中数据的条数
     */
    public int update(Line line) {
        Connection conn = getconn();
        int i = 0;
        String sql = "update t_gps_warning set warningtime_end='" + line.warningTimeEnd + "',longitude_end=" + line.lonEnd + ",latitude_end=" + line.latEnd + " where warningtime='" + line.warningTimeStart + "' and bus_code='" + line.id + "'";
        PreparedStatement pstmt;
        try {
            pstmt = conn.prepareStatement(sql);
            i = pstmt.executeUpdate();
            pstmt.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return i;
    }

    /**
     * 读取文件
     * @param Path 文件路径
     * @return line 文件最后一行
     * @throws IOException 读文件IO错误
     */
    public  String readfile(String Path) throws IOException{
        File file = new File(Path);
        FileReader fr  = new FileReader(file);
        BufferedReader br = new BufferedReader(fr);
        String s;
        String line = null;
        while((s = br.readLine()) != null){
            line = "POLYGON((" + s + "))";
        }
        br.close();
        return line;
    }

    /**
     * 创建一个元组，字段分别是开始时间，终止时间，车牌号，设备号，开始经度，开始纬度，终止经度，终止纬度，车辆颜色，车辆公司，车辆辖区局，速度，报警类型
     */
    public class Line {
        public final java.sql.Timestamp warningTimeStart;
        public final java.sql.Timestamp warningTimeEnd;
        public final String id;
        public final String device;
        public final Double lonStart;
        public final Double latStart;
        public final Double lonEnd;
        public final Double latEnd;
        public final String color;
        public final String company;
        public final String area;
        public final Integer speed;
        public final Integer type;

        public Line(Timestamp warningTimeStart, Timestamp warningTimeEnd, String id, String device, Double lonStart, Double latStart, Double lonEnd, Double latEnd, String color, String company, String area, Integer speed, Integer type) {
            this.warningTimeStart = warningTimeStart;
            this.warningTimeEnd = warningTimeEnd;
            this.id = id;
            this.device = device;
            this.lonStart = lonStart;
            this.latStart = latStart;
            this.lonEnd = lonEnd;
            this.latEnd = latEnd;
            this.color = color;
            this.company = company;
            this.area = area;
            this.speed = speed;
            this.type = type;
        }
    }
}
