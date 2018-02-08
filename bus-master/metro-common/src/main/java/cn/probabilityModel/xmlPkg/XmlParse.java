//package cn.probabilityModel.xmlPkg;
//
//import javax.xml.parsers.DocumentBuilderFactory;
//import javax.xml.parsers.DocumentBuilder;
//
//import org.apache.derby.iapi.types.XML;
//import org.w3c.dom.*;
//
//import java.io.File;
//import java.io.IOException;
//import java.util.*;
//
//import cn.ODBackModel.utils.NoNameExUtil;
//
///**
// * 解析输入的xml文件
// * 轨道交通路网信息.xml文件中的数据均存储在子节点的属性中，因此不需要考虑子节点内容
// * Created by wing1995 on 2017/7/3.
// */
//public class XmlParse {
//    //站点基本信息
//    private static Integer stationCount; //118
//    private static Integer transStation; //13
//    private static Integer repeatAllStaitonCount; //131
//    private static Integer adjacStaitonCount; //252
//    private static Integer lineCount; //5
//    private static Integer transLimitCount; //2
//
//    //站点发车间隔,有几条线路就有几个元素
//    private static List<Double> TrainInterval = new ArrayList<>();
//    //列车发车速度
//    private static List<Double> Speed = new ArrayList<>();
//    //线路编码
//    private static List<String> LineNoName = new ArrayList<>();
//
//    //中距离时间被限制在60分钟以内
//    private static int MiddleTravelLimit;
//    //短距离时间被限制在30分钟以内
//    private static int ShortTravelLimit;
//    //长距离有效路径能够增加的时间为15分钟
//    private static int LongAddLimit;
//    //中距离有效路径能够增加的时间为10分钟
//    private static int MiddleAddLimit;
//    //短距离有效路径能够增加的时间为10分钟
//    private static int ShortAddLimit;
//
//    //正态模型参数
//    private static double ShortTransPara;  //对应正态分布模型短途平峰时段的换乘时间放大系数3.390000
//    private static double ShortSumPara;  //对应正态分布模型短途平峰时段的总出行时间的放大系数3.170000
//    private static double ShortNormalPara;    //对应正态分布模型短途平峰时段的标准差系数12.610000
//    private static double MiddleTransPara; //对应正态分布模型中途平峰时段的换乘时间放大系数3.850000
//    private static double MiddleSumPara; //对应正态分布模型中途平峰时段的总出行时间的放大系数1.170000
//    private static double MiddleNormalPara;   //对应正态分布模型中途平峰时段的标准差系数3.010000
//    private static double LongTransPara;   //对应正态分布模型长途平峰时段的换乘时间放大系数2.450000
//    private static double LongSumPara;   //对应正态分布模型长途平峰时段的总出行时间的放大系数1.920000
//    private static double LongNormalPara;     //对应正态分布模型长途平峰时段的标准差系数7.050000
//
//    //站点邻接信息，key-value对应于子节点detail的属性信息
//    private static List<HashMap<String, String>> StationConnectInfor = new ArrayList<>();
//    //换乘站步行时间
//    private static List<HashMap<String, String>> TransStationWalkTime = new ArrayList<>();
//    //OD点票价
//    private static List<HashMap<String, String>> ODPrice = new ArrayList<>();
//    //换乘站点所属线路
//    private static List<HashMap<String, String>> TransStationBelongLine = new ArrayList<>();
//
//    /**
//     * 读取XML文件
//     */
//    public static void readXmlFile() throws IOException{
//
//        try {
//
//            File file = new File("E:\\trafficDataAnalysis\\probabilityModel\\导出的xml文件\\工作日-平峰-20160623.xml");
//
//            DocumentBuilder dBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
//
//            Node rootNode = dBuilder.parse(file).getChildNodes().item(0);
//
//            if (rootNode.hasChildNodes()) { //若有子节点则输出子节点名称及属性
//                getInfo(rootNode.getChildNodes());
//            }
//
//        } catch (Exception e) {
//            System.out.println(e.getMessage());
//        }
//
//    }
//
//    /**
//     * 获取节点数据信息
//     * @param nodeList 二级节点列表
//     */
//    private static void getInfo(NodeList nodeList) {
//
//        //int secondNodesNum = nodeList.getLength();
//
//        for (int count = 0; count < nodeList.getLength(); count++) { //loop in the second node
//
//            Node secondNode = nodeList.item(count);
//
//            // make sure it's element node.
//            if (secondNode.getNodeType() == Node.ELEMENT_NODE) {
//
//                // get the second node name and the third node name
//                String secondNodeName = secondNode.getNodeName();
//                NodeList thirdNodes = secondNode.getChildNodes();
//
//                // 处理三级节点
//                for (int thirdNodesNum = 0; thirdNodesNum < thirdNodes.getLength(); thirdNodesNum++) {
//
//                    if (thirdNodes.item(thirdNodesNum).getNodeType() == Node.ELEMENT_NODE) {
//
//                        if (thirdNodes.item(thirdNodesNum).hasAttributes()) {
//
//                            //初始化字典
//                            HashMap<String, String> StationConnectInforHashmap = new HashMap<>();
//                            HashMap<String, String> TransStationWalkTimeHashmap = new HashMap<>();
//                            HashMap<String, String> ODPriceHashmap = new HashMap<>();
//                            HashMap<String, String> TransStationBelongLineHashmap = new HashMap<>();
//
//                            // get the third node's attributes' names and values
//                            NamedNodeMap thirdNodeAttributes = thirdNodes.item(thirdNodesNum).getAttributes();
//
//                            for (int i = 0; i < thirdNodeAttributes.getLength(); i++) {
//
//                                Node node = thirdNodeAttributes.item(i);
//                                String attrName = node.getNodeName();
//                                String attrValue = node.getNodeValue();
//
//                                switch (secondNodeName) {
//                                    case "BaseInformation":
//                                        switch (thirdNodes.item(thirdNodesNum).getNodeName()) {
//                                            case "StaitonCount":
//                                                stationCount = Integer.parseInt(attrValue);
//                                                break;
//                                            case "TransStation":
//                                                transStation = Integer.parseInt(attrValue);
//                                                break;
//                                            case "RepeatAllStaitonCount":
//                                                repeatAllStaitonCount = Integer.parseInt(attrValue);
//                                                break;
//                                            case "AdjacStaitonCount":
//                                                adjacStaitonCount = Integer.parseInt(attrValue);
//                                                break;
//                                            case "LineCount":
//                                                lineCount = Integer.parseInt(attrValue);
//                                                break;
//                                            case "TransLimitCount":
//                                                transLimitCount = Integer.parseInt(attrValue);
//                                                break;
//                                            case "TrainInterval":
//                                                TrainInterval.add(Double.parseDouble(attrValue));
//                                                break;
//                                            case "Speed":
//                                                Speed.add(Double.parseDouble(attrValue));
//                                                break;
//                                            case "LineNoName":
//                                                LineNoName.add(attrValue);
//                                                break;
//                                            case "PathTimeSeparetor":
//                                                switch (attrName) {
//                                                    case "MiddleTravelLimit":
//                                                        MiddleTravelLimit = Integer.parseInt(attrValue);
//                                                        break;
//                                                    case "ShortTravelLimit":
//                                                        ShortTravelLimit = Integer.parseInt(attrValue);
//                                                        break;
//                                                }
//                                            case "PathAddTimeSeparetor":
//                                                switch (attrName) {
//                                                    case "LongAddLimit":
//                                                        LongAddLimit = Integer.parseInt(attrValue);
//                                                        break;
//                                                    case "MiddleAddLimit":
//                                                        MiddleAddLimit = Integer.parseInt(attrValue);
//                                                        break;
//                                                    case "ShortAddLimit":
//                                                        ShortAddLimit = Integer.parseInt(attrValue);
//                                                }
//                                            default:
//                                                break;
//                                        }
//                                        break;
//                                    case "ModelParaValue":
//                                        switch (thirdNodes.item(thirdNodesNum).getNodeName()) {
//                                            case "Short":
//                                                switch (attrName) {
//                                                    case "ShortNormalPara":
//                                                        ShortNormalPara = Double.parseDouble(attrValue);
//                                                        break;
//                                                    case "ShortSumPara":
//                                                        ShortSumPara = Double.parseDouble(attrValue);
//                                                        break;
//                                                    case "ShortTransPara":
//                                                        ShortTransPara = Double.parseDouble(attrValue);
//                                                        break;
//                                                }
//                                            case "Middle":
//                                                switch (attrName) {
//                                                    case "MiddleNormalPara":
//                                                        MiddleNormalPara = Double.parseDouble(attrValue);
//                                                        break;
//                                                    case "MiddleSumPara":
//                                                        MiddleSumPara = Double.parseDouble(attrValue);
//                                                        break;
//                                                    case "MiddleTransPara":
//                                                        MiddleTransPara = Double.parseDouble(attrValue);
//                                                }
//                                            case "Long":
//                                                switch (attrName) {
//                                                    case "LongNormalPara":
//                                                        LongNormalPara = Double.parseDouble(attrValue);
//                                                        break;
//                                                    case "LongSumPara":
//                                                        LongSumPara = Double.parseDouble(attrValue);
//                                                        break;
//                                                    case "LongTransPara":
//                                                        LongTransPara = Double.parseDouble(attrValue);
//                                                }
//                                            default:
//                                                break;
//                                        }
//                                        break;
//                                    case "StationConnectInfor":
//                                        switch (thirdNodes.item(thirdNodesNum).getNodeName()) {
//                                            case "Detail":
//                                                switch (attrName) {
//                                                    case "ClearLineNo":
//                                                        StationConnectInforHashmap.put(attrName, attrValue);
//                                                        break;
//                                                    case "IsTrans":
//                                                        StationConnectInforHashmap.put(attrName, attrValue);
//                                                        break;
//                                                    case "LineNo":
//                                                        StationConnectInforHashmap.put(attrName, attrValue);
//                                                        break;
//                                                    case "NextClearLineNo":
//                                                        StationConnectInforHashmap.put(attrName, attrValue);
//                                                        break;
//                                                    case "NextDirect":
//                                                        StationConnectInforHashmap.put(attrName, attrValue);
//                                                        break;
//                                                    case "NextStatDis":
//                                                        StationConnectInforHashmap.put(attrName, attrValue);
//                                                        break;
//                                                    case "NextStatNo":
//                                                        StationConnectInforHashmap.put(attrName, attrValue);
//                                                        break;
//                                                    case "PreClearLineNo":
//                                                        StationConnectInforHashmap.put(attrName, attrValue);
//                                                        break;
//                                                    case "PreDirect":
//                                                        StationConnectInforHashmap.put(attrName, attrValue);
//                                                        break;
//                                                    case "PreStatDis":
//                                                        StationConnectInforHashmap.put(attrName, attrValue);
//                                                        break;
//                                                    case "PreStatNo":
//                                                        StationConnectInforHashmap.put(attrName, attrValue);
//                                                        break;
//                                                    case "ServiceFee":
//                                                        StationConnectInforHashmap.put(attrName, attrValue);
//                                                        break;
//                                                    case "StatNo":
//                                                        StationConnectInforHashmap.put(attrName, attrValue);
//                                                        break;
//                                                    case "StatStopTime1":
//                                                        StationConnectInforHashmap.put(attrName, attrValue);
//                                                        break;
//                                                    case "StatStopTime2":
//                                                        StationConnectInforHashmap.put(attrName, attrValue);
//                                                        break;
//                                                    case "RunningTime1":
//                                                        StationConnectInforHashmap.put(attrName, attrValue);
//                                                        break;
//                                                    case "RunningTime2":
//                                                        StationConnectInforHashmap.put(attrName, attrValue);
//                                                        break;
//                                                }
//                                            default:
//                                                break;
//                                        }
//                                        break;
//                                    case "TransStationWalkTime":
//                                        switch (thirdNodes.item(thirdNodesNum).getNodeName()) {
//                                            case "WalkTime": {
//                                                switch (attrName) {
//                                                    case "FromLineNo":
//                                                        TransStationWalkTimeHashmap.put(attrName, attrValue);
//                                                        break;
//                                                    case "StationNo":
//                                                        TransStationWalkTimeHashmap.put(attrName, attrValue);
//                                                        break;
//                                                    case "ToLineNo":
//                                                        TransStationWalkTimeHashmap.put(attrName, attrValue);
//                                                        break;
//                                                    case "direction00":
//                                                        TransStationWalkTimeHashmap.put(attrName, attrValue);
//                                                        break;
//                                                    case "direction01":
//                                                        TransStationWalkTimeHashmap.put(attrName, attrValue);
//                                                        break;
//                                                    case "direction10":
//                                                        TransStationWalkTimeHashmap.put(attrName, attrValue);
//                                                        break;
//                                                    case "direction11":
//                                                        TransStationWalkTimeHashmap.put(attrName, attrValue);
//                                                        break;
//                                                    default:
//                                                        break;
//                                                }
//                                            }
//                                        }
//                                        break;
//                                    case "ODPrice":
//                                        switch (thirdNodes.item(thirdNodesNum).getNodeName()) {
//                                            case "Price":
//                                                switch (attrName) {
//                                                    case "Ori":
//                                                        ODPriceHashmap.put(attrName, attrValue);
//                                                        break;
//                                                    case "Des":
//                                                        ODPriceHashmap.put(attrName, attrValue);
//                                                        break;
//                                                    case "Price":
//                                                        ODPriceHashmap.put(attrName, attrValue);
//                                                        break;
//                                                }
//                                        }
//                                        break;
//                                    case "TransStationBelongLine":
//                                        switch (thirdNodes.item(thirdNodesNum).getNodeName()) {
//                                            case "TransStationBelongLineInfor":
//                                                switch (attrName) {
//                                                    case "StationNo":
//                                                        TransStationBelongLineHashmap.put(attrName, attrValue);
//                                                        break;
//                                                    case "BelongToLine":
//                                                        TransStationBelongLineHashmap.put(attrName, attrValue);
//                                                        break;
//                                                    default:
//                                                        break;
//                                                }
//                                        }
//                                    break;
//                                }
//                            }
//                            if (StationConnectInforHashmap.size() != 0) {
//                                StationConnectInfor.add(StationConnectInforHashmap);
//                            }
//                            if (TransStationWalkTimeHashmap.size() != 0) {
//                                TransStationWalkTime.add(TransStationWalkTimeHashmap);
//                            }
//                            if (ODPriceHashmap.size() != 0) {
//                                ODPrice.add(ODPriceHashmap);
//                            }
//                            if (TransStationBelongLineHashmap.size() != 0) {
//                                TransStationBelongLine.add(TransStationBelongLineHashmap);
//                            }
//                        }
//                    }
//                }
//            }
//        }
//    }
//
//    public static XmlParse xmlParse; //实例化对象
//    static  {
//        xmlParse = new XmlParse();
//        try {
//            readXmlFile();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    public static List<HashMap<String, String>> getStationConnectInfor() {
//        return StationConnectInfor;
//    }
//
//    public static Integer getStationCount() {
//        return stationCount;
//    }
//
//    /**
//     * 测试函数
//     * @param args 参数
//     */
//    public static void main(String args[]) {
////        readXmlFile();
////        for (Map.Entry<String, String> map: StationConnectInfor.get(0).entrySet()) {
////            System.out.println(map.getKey() + ":"  + map.getValue());
////        }
////        System.out.println();
////        for (Map.Entry<String, String> map: TransStationWalkTime.get(0).entrySet()) {
////            System.out.println(map.getKey() + ":"  + map.getValue());
////        }
////        System.out.println();
//////        for (Map.Entry<String, String> map: ODPrice.get(0).entrySet()) {
//////            System.out.println(map.getKey() + ":"  + map.getValue());
//////        }
////        System.out.println();
////        for (Map.Entry<String, String> map: TransStationBelongLine.get(0).entrySet()) {
////            System.out.println(map.getKey() + ":"  + map.getValue());
////        }
////        getStationConnectInfor();
//
//        System.out.println(NoNameExUtil.NO2Name(getStationConnectInfor().get(0).get("StatNo")));
//    }
//}
