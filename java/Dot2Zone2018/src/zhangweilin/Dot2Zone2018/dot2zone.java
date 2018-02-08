package zhangweilin.Dot2Zone2018;

import java.io.IOException;
import cn.sibat.metro.truckOD.ParseShp;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;



public class dot2zone extends EvalFunc<String> {



    public String exec(Tuple input) {

        if(input == null || input.size() == 0)
            return null;
            try {
                String shpPath = (String) input.get(0);
                Double lon = (Double) input.get(1);
                Double lat = (Double) input.get(2);

                String zone= new ParseShp(shpPath).readShp().getZoneName(lon,lat);
                return zone;
            } catch (ExecException e) {
                e.printStackTrace();
            }
            return null;
    }
    public static void main(String[] args){
        String zone = new ParseShp("E:/trafficDataAnalysis/Guotu/行政区划2017/英文/xzq.shp").readShp().getZoneName(114.245,22.722);
        System.out.println(zone);
    }
}


/*
public class Dot2Zone{

	public static void main(String[] arg0){
		 String zone=LocationService.locate(114.29398148582718,22.748611);
		 System.out.println(zone);


	}
*/



