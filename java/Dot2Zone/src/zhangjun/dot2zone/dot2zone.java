package zhangjun.dot2zone;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;


import storm.realTraffic.citydivided.LocationService;


/*
public class dot2zone extends EvalFunc<String> {

    

    public String exec(Tuple input) throws IOException {               
   
   			 
   	 
   	 if(input == null || input.size() == 0)  
            return null;
        try {  
            Double lon = (Double) input.get(0);
            Double lat = (Double) input.get(1);
            String zone=LocationService.locate(lon,lat);
            return zone;
                         
                    	 
        } catch (Exception e) {  
            throw new IOException(e.getMessage());  
        }  
    }  
  

    }
*/


public class dot2zone{
	
	public static void main(String[] arg0){
		 String zone=LocationService.locate(114.29398148582718,22.748611);
		 System.out.println(zone);
		
		
	} 
	
	
}

