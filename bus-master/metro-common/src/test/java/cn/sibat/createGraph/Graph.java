package cn.sibat.createGraph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * This class models a sample. undirected graph using
 * an incidence list representation. 使用关联列表表示无向图
 * Created by wing1995 on 2017/6/28.
 */
public class Graph {
    private HashMap<String, Vertex> vertices;
    private HashMap<Integer, Edge> edges;

    public Graph() {
        this.vertices = new HashMap<String, Vertex>();
        this.edges = new HashMap<Integer, Edge>();
    }

    public Graph(ArrayList<Vertex> vertices) {
        this.vertices = new HashMap<String, Vertex>();
        this.edges = new HashMap<Integer, Edge>();

        for(Vertex v: vertices) {
            this.vertices.put(v.getLabel(), v);
        }
    }

    public boolean addEdge(Vertex one, Vertex two, int weight){
        //无环
        if(one.equals(two)) {
            return false;
        }

        //ensures the Edge is not in the Graph
        Edge e = new Edge(one, two, weight);
        if(edges.containsKey(e.hashCode())){
            return false;
        }

        //and that the Edge isn't already incident to one of the vertexes
        else if(one.containsNeighbor(e)) {
            return false;
        }
        //只给第一个节点添加邻居
        edges.put(e.hashCode(), e);
        one.addNeighbor(e);
        return true;
    }

    public boolean containsEdge(Edge e){
        if(e.getOne() == null || e.getTwo() == null){
            return false;
        }
        return this.edges.containsKey(e.hashCode());
    }

    public Edge removeEdge(Edge e){
        e.getOne().removeNeighbor(e);
        e.getTwo().removeNeighbor(e);
        return this.edges.remove(e.hashCode());
    }

    public boolean containsVertex(Vertex vertex) {
        return this.vertices.get(vertex.getLabel()) != null;
    }

    public Vertex getVertex(String label){
        return vertices.get(label);
    }

    public boolean addVertex(Vertex vertex){
        vertices.put(vertex.getLabel(), vertex);
        return true;
    }

    public Set<String> vertexKeys(){
        return this.vertices.keySet();
    }

    public Set<Edge> getEdges(){
        return new HashSet<Edge>(this.edges.values());
    }

}
