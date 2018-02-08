//package cn.probabilityModel.graphPkg;
//
//import java.util.HashMap;
//import java.util.List;
//
//import cn.ODBackModel.utils.NoNameExUtil;
//import cn.probabilityModel.xmlPkg.XmlParse;
//
//import org.jgrapht.alg.interfaces.ShortestPathAlgorithm;
//import org.jgrapht.alg.shortestpath.DijkstraShortestPath;
//import org.jgrapht.graph.*;
//
//
///**
// * 使用JGraphT求最短距离，注意JGraphT中的Dijkstra中的最短距离由斐波那契搜索到
// * Create the shortest path according to the shortestPath's algorithm
// * Created by wing1995 on 2017/6/28.
// */
//public class ShortestPath {
//    //获取数据
//    private static Integer stationCount = XmlParse.getStationCount();
//    private static List<HashMap<String, String>> stationConnectInfor = XmlParse.getStationConnectInfor();
//
////    /**
////     * 计算相邻站点之间的出行时间，不同方向时间不同，不同情况出行时间也不同
////     * @return costTime
////     */
////    public static Double getCostTime(String source, String destination) {
////        Double costTime;
////    }
//
//    /**
//     * 创建加权有向图
//     * @return metroGraph 地铁连通图
//     */
//    public static SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> createGraph() {
//        //构建一个加权有向图
//        SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> metroGraph = new SimpleDirectedWeightedGraph<>(DefaultWeightedEdge.class);
//
////        //创建边和点以及权重
////        for(HashMap<String, String> stationInfo: stationConnectInfor){
////
////            String fromStation = NoNameExUtil.NO2Ser(stationInfo.get("StatNo"));
////            String toStation = NoNameExUtil.NO2Ser(stationInfo.get("NextStatNo"));
////            Double costTime = Double.parseDouble(stationInfo.get("RunningTime2"));
//
//            metroGraph.addVertex(fromStation);
//
//            if (toStation != null) {
//                metroGraph.addVertex(toStation);
//                DefaultWeightedEdge metroEdge = metroGraph.addEdge(fromStation, toStation);
//                metroGraph.setEdgeWeight(metroEdge, costTime);
//            }
//        }
//        return metroGraph;
//    }
//
//    /**
//     * 获取任意点到其他站点的最短时间
//     * @return shortTimeMatrix
//     */
//    public static Double[][] getShortestPath() {
//
//        SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> metroGraph = createGraph();
//
//        DijkstraShortestPath<String, DefaultWeightedEdge> dijkstraAlg = new DijkstraShortestPath<>(metroGraph);
//
//        Double shortTimeMatrix[][] = new Double[stationCount][stationCount]; //存储最短时间二维矩阵
//
//        for(int stationRow = 1; stationRow <= stationCount; stationRow++) {
//            ShortestPathAlgorithm.SingleSourcePaths<String, DefaultWeightedEdge> iPaths = dijkstraAlg.getPaths(Integer.toString(stationRow));
//            for(int stationCol = 1; stationCol <= stationCount; stationCol++) {
//                Double shortTime = iPaths.getWeight("" + stationCol);
//                shortTimeMatrix[stationRow - 1][stationCol - 1] = shortTime;
//            }
//        }
//        return shortTimeMatrix;
//    }
//
//    public static void main(String args[]) {
//        Double[][] shortTimeMatrix = getShortestPath();
//        System.out.println(shortTimeMatrix[0][2]);
//    }
//}