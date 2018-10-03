# SparkSQLDemo
## Person.java
一个实体类，有ID和Name属性，有对应get和set方法。
## SparkSqlTest1.java
* 1）将字符串数组转换成JavaRDD<String>类型
* 2）将JavaRDD<String>转换成JavaRDD<Person>类型
* 3）将JavaRDD<Person>类型数据在内存建表，写SQL语句查询。
### 代码及结果
* 1）代码：
```Java
line1.foreach(new VoidFunction<String>(){
            @Override
            public void call(String num) throws Exception {
                System.out.println("numbers;"+num);
            }
        });
        
```
```Java
Dataset<Row> stuDf = sqlContext.createDataFrame(stuRDD, Person.class);
		stuDf.printSchema();
		stuDf.createOrReplaceTempView("Person");
		Dataset<Row> nameDf = sqlContext.sql("select * from Person ");
		nameDf.show(); 
```
* 2）结果：<br>
  ![](https://github.com/superxinxin/SparkSQLDemo/blob/master/Images/%E6%8D%95%E8%8E%B71.PNG)
  ![](https://github.com/superxinxin/SparkSQLDemo/blob/master/Images/%E6%8D%95%E8%8E%B72.PNG)
## SparkSqlTest2.java
* 与SparkSqlTest1.java代码类似功能，给出了更多输入源及实现方式。
* 第一种方式：将ArrayList生成RDD，然后按下面步骤1,2,3进行
  * 步骤1.1：在RDD的基础上创建类型为Row的RDD；
  * 步骤1.2：构建字段数据名称和类型；
  * 步骤1.3：建表，写SQL语句查询。
* 第二种方式：从text文件读入成RDD，生成DataFrame，生成临时表people，用SQL语句查询；
* 第三种方式：读jdbc方式获得Dataset；
* 第四种方式：读json方式获得Dataset 。
* 具体可以看代码，已写注释。
