# ETLexample


This is examples of my ETL processes made by Spark on Scala. 

This ETL process processes data on tax transactions. We take the initial layer, perform aggregating operations on it like
Filtering, adding new columns, joins. After that, the data is written to HDFS. The result is a tax report for the internal services of the bank.

Inside you can find next objects and classes:

1.0 **srs/main/scala**

1.1 Application

Main app. It accepts spark arguments and addition arguments like startDate, endDate, asnuCode. These information 
is needed to perform correct ETL operation. Concurrentthought.cla library is used to parse these arguments.
asnuCode is internal technical code of bank. When asnuCode is 6000 it is called "EKP" number, when it is 9000 it is called "EKS" number.

1.2 ApplicationArguments

This class is neede for  Concurrentthought.cla to create new arguments for main app

1.3 Const

Constants which are used in this ETL process

1.4 DataSources

Here you can find methods which reads initial dataframes

1.5 EKP and EKS masks

Each tax transactions has dt_acoount ad kt_account number. During ETL proccess we need to filter these transactions
according to mask. Mask is first 5 numbers of dt_account or kt_account number. 

1.6 Sheet30AggregationTransform

Here you can find transform method which performs all aggrigation functions. According to asnuCode it chooses 
right process. 

2.0 **srs/main/test**

2.1 AggregationEKP30Test

Here are tests for EKP process. First test is called main app test and it just start main app with arguments.
Next tests check that all filters and aggrigations works correct. We know for sure that transactions with certain
masks should pass all filters and aggrigations and stays in final dataframe. We crating a case class which represent one tax transaction
and performing transform operation on it. We check all combinations of masks. 
If we get empty dataframe after ETL process it means that test is failed.

2.2 DTO
case class for tests

3.0 **res**

3.1 in

initial dataframs

3.2 out

folder where final dataframe will be written







