package zhangjun.TimeChange;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

 

public class change15min extends EvalFunc<String> {

        

         public String exec(Tuple input) throws IOException {               
        
        			 
        	 
        	 if(input == null || input.size() == 0)  
                 return null;
             try {  
                 String val = (String) input.get(0);
                 String append1 = "00:00";
                 String append2 = "15:00";
                 String append3 = "30:00";
                 String append4 = "45:00";
                 String t1;
                 String t2;
              
                 
                 t1 = val.substring(0,14);
                 t2 = val.substring(14,16);
                 Integer temp = Integer.valueOf(t2);
                 if(temp>=0 && temp<15){
                	 return t1+append1;
                 }
                 else if(temp>=15 && temp<30){
                	 return t1+append2;
                 }
                 else if(temp>=30 && temp<45){
                	 return t1+append3;
                 }
                 else {
                	 return t1+append4;
                 }
                 
                         	 
             } catch (Exception e) {  
                 throw new IOException(e.getMessage());  
             }  
         }  
       

         }

