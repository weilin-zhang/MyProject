package zhangjun.TimeChange;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

 

public class Change2ISO8601 extends EvalFunc<String> {

        

         public String exec(Tuple input) throws IOException {               
        
        			 
        	 
        	 if(input == null || input.size() == 0)  
                 return null;
             try {  
                 String val1 = (String) input.get(0);
                 String val2 = (String) input.get(1);
                 String t;
                 if(val2.length()==8)
                 {
                	
                	 t = val1 + "T" + val2;
                	 return t;
                	 
                	 
                 }
                 else
                 {
             
                	
                	 t = val1 + "T" + "0" + val2;
                     return t;
                
                	 
                 }
              
                         	 
             } catch (Exception e) {  
                 throw new IOException(e.getMessage());  
             }  
         }  
       

         }

