package sparkSQL1;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
public class SparkSqlTest1
{
	public static void main(String[] args)
	{
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Simple Application");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		// convert from other RDD
		JavaRDD<String> line1 = sc.parallelize(Arrays.asList("1 aa", "2 bb", "4 cc", "3 dd"));
		line1.foreach(new VoidFunction<String>()
		{
			@Override
			public void call(String num) throws Exception
			{
				// TODO Auto-generated method stub
				System.out.println("numbers:" + num);
			}
		});
		JavaRDD<Person> stuRDD = line1.map(new Function<String, Person>()
		{
			public Person call(String line) throws Exception
			{
				String[] lineSplit = line.split(" ");
				Person per = new Person();
				per.setId(lineSplit[0]);
				per.setName(lineSplit[1]);
				return per;
			}
		});
		Dataset<Row> stuDf = sqlContext.createDataFrame(stuRDD, Person.class);
		// stuDf.select("id","name","age").write().mode(SaveMode.Append).parquet("par");
		// //对文件指定列名
		stuDf.printSchema();
		stuDf.createOrReplaceTempView("Person");
		Dataset<Row> nameDf = sqlContext.sql("select * from Person ");
		nameDf.show(); 
	}
}
