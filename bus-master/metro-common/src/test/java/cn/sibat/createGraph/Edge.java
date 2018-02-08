package cn.sibat.createGraph;

/**
 * This class models an directed Edge in the Graph
 * Created by wing1995 on 2017/6/28.
 */
public class Edge implements Comparable<Edge>{
    private Vertex one, two;
    private int weight;

    /**
     * 有向加权图
     * @param one The first vertex in the Edge
     * @param two The second vertex of the Edge
     * @param weight The weight of this Edge
     */
    public Edge(Vertex one, Vertex two, int weight) {
        this.one = one;
        this.two = two;
        this.weight = weight;
    }

    /**
     * 只返回下一节点
     * @param current current edge
     * @return The neighbor of current along this edge
     */
    public Vertex getNeighbor(Vertex current) {
        if(!(current.equals(one))) {
            return null;
        }
        return two;
    }

    /**
     *
     * @return Vertex this.one
     */
    public Vertex getOne() {
        return this.one;
    }

    /**
     *
     * @return Vertex this.two
     */
    public Vertex getTwo(){
        return this.two;
    }

    /**
     *
     * @return weight The weight of this Edge
     */
    public int getWeight(){
        return this.weight;
    }

    /**
     *
     * @param other The Edge to compare against this Edge
     * @return int this.weight - other.weight
     */
    public int compareTo(Edge other){
        return this.weight - other.weight;
    }

    /**
     *
     * @return String A String representation of the Edge
     */
    public String toString() {
        return "\n从" + one +  "到" + two + "耗时" + weight + "秒";
    }

    /**
     *
     * @return int The hash mainPkg for this Edge
     */
    public int hashCode(){
        return (one.getLabel() + two.getLabel()).hashCode();
    }

    /**
     *
     * @param other The Object to compare against this
     * @return true if other is an Edge with the same Vertex as this
     */
    public boolean equals(Object other){
        if(!(other  instanceof Edge)){
            return false;
        }
        Edge e = (Edge)other;
        return e.one.equals(this.one) && e.two.equals(this.two);
    }
}
