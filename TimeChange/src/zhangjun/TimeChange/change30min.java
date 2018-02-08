package zhangjun.TimeChange;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

 

public class change30min extends EvalFunc<String> {

        

         public String exec(Tuple input) throws IOException {               
        
        			 
        	 
        	 if(input == null || input.size() == 0)  
                 return null;
             try {  
                 String val = (String) input.get(0);
                 String append1 = "00:00";
                 String append2 = "30:00";
                 String t1;
                 String t2;
                 
                 t1 = val.substring(14,15);
                 t2 = val.substring(0,14);
                 
                 if(t1.equals("0")||t1.equals("1")||t1.equals("2"))
                 {
                	 
                	 return t2 + append1;
                	 
                 }
                 
                 else
                 {
                	 
                	 return t2 + append2;
                	 
                 }
                
                         	 
             } catch (Exception e) {  
                 throw new IOException(e.getMessage());  
             }  
         }  
       

         }

