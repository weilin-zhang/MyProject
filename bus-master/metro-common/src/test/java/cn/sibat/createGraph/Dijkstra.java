//package cn.sibat.createGraph;
//
//import java.util.*;
//
////import cn.ODBackModel.utils.NoNameExUtil;
////import cn.probabilityModel.xmlPkg.XmlParse;
//
///**
// * 使用二分法搜索二叉树的最短距离，利用PriorityQueue实现
// * Create the shortest path according to the shortestPath's algorithm
// * Created by wing1995 on 2017/6/28.
// */
//public class Dijkstra {
//
//   private Graph graph;
//   private String initialVertexLabel; //初始节点
//   private HashMap<String, String> predecessors; //某一节点的上一个节点
//   private HashMap<String, Integer> distances; //某节点距离初始节点的距离
//   private PriorityQueue<Vertex> availableVertices; //可获取的节点优先级队列
//   private HashSet<Vertex> visitedVertices; //已经被访问点集合
//
//    /**
//     *
//     * @param graph the Graph to traverse
//     * @param initialVertexLabel The starting Vertex label
//     */
//   public Dijkstra(Graph graph, String initialVertexLabel){
//       this.graph = graph;
//       Set<String> vertexKeys = this.graph.vertexKeys();
//
//       if(!vertexKeys.contains(initialVertexLabel)){
//           throw new IllegalArgumentException("The graph must contain the initial vertex.");
//       }
//       //初始化
//       this.initialVertexLabel = initialVertexLabel;
//       this.predecessors = new HashMap<>();
//       this.distances = new HashMap<>();
//       this.availableVertices = new PriorityQueue<Vertex>(vertexKeys.size(), new Comparator<Vertex>() {
//           @Override
//           public int compare(Vertex one, Vertex two) {
//               int weightOne = Dijkstra.this.distances.get(one.getLabel());
//               int weightTwo = Dijkstra.this.distances.get(two.getLabel());
//               return weightOne - weightTwo;
//           }
//       }); //可获取的节点
//
//       this.visitedVertices = new HashSet<Vertex>(); //初始化已被访问的节点
//       //For each vertex in the graph, assume it has distance infinity denoted by Integer.MAX_VALUE
//       for(String key: vertexKeys){
//           this.predecessors.put(key, null);
//           this.distances.put(key, Integer.MAX_VALUE);
//       }
//
//       //The distance from the initial vertex to itself is 0
//       this.distances.put(initialVertexLabel, 0);
//
//       //seed initialVertex's neighbors
//       Vertex initialVertex = this.graph.getVertex(initialVertexLabel);
//       ArrayList<Edge> initialVertexNeighbors = initialVertex.getNeighbors();
//       for(Edge e: initialVertexNeighbors){
//           Vertex other = e.getNeighbor(initialVertex);
//           this.predecessors.put(other.getLabel(), initialVertexLabel);
//           this.distances.put(other.getLabel(), e.getWeight());
//           this.availableVertices.add(other);
//       }
//
//       this.visitedVertices.add(initialVertex);
//
//       //Apply shortestPath's algorithm to the Graph
//       processGraph();
//   }
//
//    /**
//     * This method applies shortestPath's algorithm to the graph
//     * using the Vertex specified by initialVertexLabel as the starting point
//     */
//   private void processGraph(){
//
//       while (this.availableVertices.size() > 0) {
//           //Pick the closest vertex
//           Vertex next = this.availableVertices.poll();
//
//           int distanceToNext = this.distances.get(next.getLabel());
//
//           //For each available neighbor of the chosen vertex
//           List<Edge> nextNeighbors = next.getNeighbors();
//           for(Edge e: nextNeighbors){
//               Vertex other = e.getNeighbor(next);
//               if(this.visitedVertices.contains(other)){
//                   continue;
//               }
//               //Calculate every unvisited neighbor's distance
//               int currentWeight = this.distances.get(other.getLabel());
//               int newWeight = distanceToNext + e.getWeight();
//
//               if(newWeight < currentWeight) {
//                   this.predecessors.put(other.getLabel(), next.getLabel());
//                   this.distances.put(other.getLabel(), newWeight);
//                   //更新优先级队列
//                   this.availableVertices.add(other);
//               }
//           }
//           //Label the next node as visited, and go to the next available vertex
//           this.visitedVertices.add(next);
//       }
//   }
//
//    /**
//     *
//     * @param destinationLabel The Vertex whose shortest path from the initial Vertex is desired
//     * @return LinkedList<Vertex> A sequence of Vertex objects starting at initial Vertex and terminating at the Vertex specified by destinationLabel.
//     *         The path is the shortest path specified by shortestPath's algorithm.
//     */
//   public List<Vertex> getPathTo(String destinationLabel){
//       LinkedList<Vertex> path = new LinkedList<Vertex>();
//       path.add(graph.getVertex(destinationLabel));
//
//       while(!destinationLabel.equals(this.initialVertexLabel)){
//           Vertex predecessor = graph.getVertex(this.predecessors.get(destinationLabel));
//           destinationLabel = predecessor.getLabel();
//           path.add(0, predecessor);
//       }
//       return path;
//   }
//
//    /**
//     *
//     * @param destinationLabel The Vertex to determine from the initial Vertex
//     * @return int The distance from the initial Vertex to the Vertex specified by destinationLabel
//     */
//   public int getDistanceTo(String destinationLabel){
//       return this.distances.get(destinationLabel);
//   }
//
//   public static void main(String[] args){
//
//       Graph graph = new Graph();
//       List<HashMap<String, String>> stationConnectInfor =  XmlParse.getStationConnectInfor();
//
//       for(HashMap<String, String> stationInfo: stationConnectInfor){
//
////           String fromStation = NoNameExUtil.NO2Name(stationInfo.get("StatNo"));
////           String toStation = NoNameExUtil.NO2Name(stationInfo.get("NextStatNo"));
//           Integer costTime = Integer.parseInt(stationInfo.get("RunningTime2"));
//
//           Vertex fromVertex = new Vertex(fromStation);
//           Vertex toVertex = new Vertex(toStation);
//
//           graph.addVertex(fromVertex);
//           graph.addVertex(toVertex);
//
//           if (toStation != null) {
//
//               graph.addEdge(fromVertex, toVertex, costTime);
//           }
//       }
//
//       System.out.println(graph.getEdges().toString().substring(1, graph.getEdges().toString().length()-1));
//
//       Dijkstra dijkstra = new Dijkstra(graph, "后海");
//       System.out.println(dijkstra.getDistanceTo("景田"));
//       System.out.println(dijkstra.getPathTo("景田"));
//   }
//}
