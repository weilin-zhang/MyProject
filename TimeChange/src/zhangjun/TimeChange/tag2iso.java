package zhangjun.TimeChange;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

 

public class tag2iso extends EvalFunc<String> {

        

         public String exec(Tuple input) throws IOException {               
        
        			 
        	 
        	 if(input == null || input.size() == 0)  
                 return null;
             try {  
                 String val = (String) input.get(0);
                 String append = ".000Z";
                 String t;
                 
                 t = val.substring(0,19);
                 
                 return t + append;
                         	 
             } catch (Exception e) {  
                 throw new IOException(e.getMessage());  
             }  
         }  
       

         }

