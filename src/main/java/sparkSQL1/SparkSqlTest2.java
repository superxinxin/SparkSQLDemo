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
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
public class SparkSqlTest2
{
	public static void main(String[] args)
	{
		SparkSession spark=SparkSession.builder()  
                .appName("RDDToDataset")  
                .master("local[*]")  
                .getOrCreate();  

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        /*
         *第一种方式：将ArrayList生成RDD，然后按下面步骤1.1; 1.2 ;1.3进行         
         */
        List<String> numberList = Arrays.asList("1 aa", "2 bb", "4 cc", "3 dd");
        JavaRDD<String> line1 = sc.parallelize(numberList);
        /*
         *第二种方式：从text文件读入成RDD，生成DataFrame，生成临时表people，用SQL语句查询
         */
//        JavaRDD<Person> peopleRDD = spark.read()
//        		  .textFile("examples/src/main/resources/people.txt")
//        		  .javaRDD()
//        		  .map(line -> {
//        		    String[] parts = line.split(",");
//        		    Person person = new Person();
//        		    person.setName(parts[0]);
//        		    person.setAge(Integer.parseInt(parts[1].trim()));
//        		    return person;
//        		  });
//        		// Apply a schema to an RDD of JavaBeans to get a DataFrame
//        		Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Person.class);
//        		// Register the DataFrame as a temporary view
//        		peopleDF.createOrReplaceTempView("people");
//        		// SQL statements can be run by using the sql methods provided by spark
//        		Dataset<Row> teenagersDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19");

        line1.foreach(new VoidFunction<String>(){
            @Override
            public void call(String num) throws Exception {
                System.out.println("numbers;"+num);
            }
        });
		  
         //步骤1.1：在RDD的基础上创建类型为Row的RDD
        //首先，必须将RDD变成以Row为类型的RDD。Row可以简单理解为Table的一行数据
       JavaRDD<Row> personsRDD = line1.map(new Function<String,Row>(){
            @Override
            public Row call(String line) throws Exception {
                String[] splited = line.split(" ");
                return RowFactory.create(splited[0],splited[1]);
            }
        });
       //步骤1.2：构建字段数据名称和类型
        List<StructField> fields=new ArrayList<StructField>();  
        fields.add(DataTypes.createStructField("id", DataTypes.StringType, true));  
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));  

        //注意：dataframe更像是一张关系型数据表，是一种spark独有的数据格式，这种格式的数据可以使用sqlcontext里面的函数
        StructType schema=DataTypes.createStructType(fields);  
        Dataset stuDf=spark.createDataFrame(personsRDD, schema);
        /*
         *第三种方式：读jdbc方式获得Dataset       
         */
//      Dataset styDf2 = spark.read().jdbc(url, table, properties);
        /*
         *第四种方式：读json方式获得Dataset         
         */
//      Dataset styDf3 = spark.read().json(path);
        
        stuDf.printSchema();  
        //步骤1.3：建表，写SQL语句查询
        stuDf.createOrReplaceTempView("Person");  
        Dataset<Row> nameDf=spark.sql("select * from Person where id=3");  
        nameDf.show();  
	}
}
