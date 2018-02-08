package zhangjun.TimeChange;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

 

public class year2year_period extends EvalFunc<DataBag> {

        

         public DataBag exec(Tuple input) throws IOException {               
        
        			 
        	 
        	 if(input == null || input.size() == 0)  
                 return null;
             try {  
                 String val = (String) input.get(0);
                 String append1 = "T00:00:00";
                 String append2 = "T01:00:00";
                 String append3 = "T02:00:00";
                 String append4 = "T03:00:00";
                 String append5 = "T04:00:00";
                 String append6 = "T05:00:00";
                 String append7 = "T06:00:00";
                 String append8 = "T07:00:00";
                 String append9 = "T08:00:00";
                 String append10 = "T09:00:00";
                 String append11 = "T10:00:00";
                 String append12 = "T11:00:00";
                 String append13 = "T12:00:00";
                 String append14 = "T13:00:00";
                 String append15 = "T14:00:00";
                 String append16 = "T15:00:00";
                 String append17 = "T16:00:00";
                 String append18 = "T17:00:00";
                 String append19 = "T18:00:00";
                 String append20 = "T19:00:00";
                 String append21 = "T20:00:00";
                 String append22 = "T21:00:00";
                 String append23 = "T22:00:00";
                 String append24 = "T23:00:00";
              
                 BagFactory bagfactory = BagFactory.getInstance();
                 DataBag bag = bagfactory.newDefaultBag();                           
                 TupleFactory tuplefactory = TupleFactory.getInstance();
                 
                 Tuple tt1 = tuplefactory.newTuple(1);
                 Tuple tt2 = tuplefactory.newTuple(1);
                 Tuple tt3 = tuplefactory.newTuple(1);
                 Tuple tt4 = tuplefactory.newTuple(1);
                 Tuple tt5 = tuplefactory.newTuple(1);
                 Tuple tt6 = tuplefactory.newTuple(1);
                 Tuple tt7 = tuplefactory.newTuple(1);
                 Tuple tt8 = tuplefactory.newTuple(1);
                 Tuple tt9 = tuplefactory.newTuple(1);
                 Tuple tt10 = tuplefactory.newTuple(1);
                 Tuple tt11 = tuplefactory.newTuple(1);
                 Tuple tt12 = tuplefactory.newTuple(1);
                 Tuple tt13 = tuplefactory.newTuple(1);
                 Tuple tt14 = tuplefactory.newTuple(1);
                 Tuple tt15 = tuplefactory.newTuple(1);
                 Tuple tt16 = tuplefactory.newTuple(1);
                 Tuple tt17 = tuplefactory.newTuple(1);
                 Tuple tt18 = tuplefactory.newTuple(1);
                 Tuple tt19 = tuplefactory.newTuple(1);
                 Tuple tt20 = tuplefactory.newTuple(1);
                 Tuple tt21 = tuplefactory.newTuple(1);
                 Tuple tt22 = tuplefactory.newTuple(1);
                 Tuple tt23 = tuplefactory.newTuple(1);
                 Tuple tt24 = tuplefactory.newTuple(1);
                 
                 
           	    
                 tt1.set(0, val+append1);                
                 bag.add(tt1);
                 tt2.set(0, val+append2);                
                 bag.add(tt2);
                 tt3.set(0, val+append3);                
                 bag.add(tt3);
                 tt4.set(0, val+append4);                
                 bag.add(tt4);
                 tt5.set(0, val+append5);                
                 bag.add(tt5);
                 tt6.set(0, val+append6);                
                 bag.add(tt6);
                 tt7.set(0, val+append7);                
                 bag.add(tt7);
                 tt8.set(0, val+append8);                
                 bag.add(tt8);
                 tt9.set(0, val+append9);                
                 bag.add(tt9);
                 tt10.set(0, val+append10);                
                 bag.add(tt10);
                 tt11.set(0, val+append11);                
                 bag.add(tt11);
                 tt12.set(0, val+append12);                
                 bag.add(tt12);
                 tt13.set(0, val+append13);                
                 bag.add(tt13);
                 tt14.set(0, val+append14);                
                 bag.add(tt14);
                 tt15.set(0, val+append15);                
                 bag.add(tt15);
                 tt16.set(0, val+append16);                
                 bag.add(tt16);
                 tt17.set(0, val+append17);                
                 bag.add(tt17);
                 tt18.set(0, val+append18);                
                 bag.add(tt18);
                 tt19.set(0, val+append19);                
                 bag.add(tt19);
                 tt20.set(0, val+append20);                
                 bag.add(tt20);
                 tt21.set(0, val+append21);                
                 bag.add(tt21);
                 tt22.set(0, val+append22);                
                 bag.add(tt22);
                 tt23.set(0, val+append23);                
                 bag.add(tt23);
                 tt24.set(0, val+append24);                
                 bag.add(tt24);
                 return bag;
                
                         	 
             } catch (Exception e) {  
                 throw new IOException(e.getMessage());  
             }  
         }  
       

         }

