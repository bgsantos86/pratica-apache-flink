package br.com.pucpr.FlinkPratica;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction;

/**
 * Hello world!
 *
 */
public class FlinkMain 
{
    public static void main( String[] args ) throws Exception
    {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        
        DataStream<Integer> inteiros = see.fromElements(1, 1, 1, 2, 2, 2, 3, 3, 3, 4);
        
        inteiros.countWindowAll(3).reduce(new AggregationFunction<Integer>() {
			private static final long serialVersionUID = 1L;

			public Integer reduce(Integer value1, Integer value2) throws Exception {
				return value1 + value2	;
			}
        	
        }).map(new MapFunction<Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			public Integer map(Integer value) throws Exception {
				return value * 2;
			}        
        }).filter(new FilterFunction<Integer>() {
			private static final long serialVersionUID = 1L;

			public boolean filter(Integer value) throws Exception {
				return (value > 10);
			}
		}).print();
        
        see.execute();
        
    }
}
