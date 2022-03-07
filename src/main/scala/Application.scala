import ApplicationArguments._
import com.concurrentthought.cla.Args
import org.apache.hadoop.fs.Path
import org.slf4j.{Logger, LoggerFactory}


object Application {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    logger.info("[TECH] Start sheet 30 aggregation")
    val appArgs: Args = ApplicationArguments.initialArgs(args).process(args)

    logger.info(s"[TECH] Application arguments: ${appArgs.allValues}")

    //parsing arguments
    val asnuCode = getOrException[String](appArgs, SYSTEM_CODE, required = true)
    val startDate: String = getOrException[String](appArgs, START_DATE, required = true)
    val endDate: String = getOrException[String](appArgs, END_DATE, required = true)
    val hdfsReportPath : String = getOrException[String](appArgs, HDFS_PATH, required = true)



    logger.info(s"[TECH] Got startDate=$startDate, endDate=$endDate")

    // getting initial dataframes from source
    val dataSources = new DataSources()
    val actionSheetDf = dataSources.getActionSheet30Df(asnuCode)
    val essFilialMap = dataSources.getFilialMappingDf
    val paymentPurposeDf = dataSources.getPaymentPurposeDf()

    // performing ETL operation
    val df = Sheet30AggregationTransform.transform(
      actionSheetDf = actionSheetDf,
      startDate = startDate,
      endDate = endDate,
      asnuCode = asnuCode,
      filialMapDf = essFilialMap,
      paymentPurposeDf = paymentPurposeDf
    )

    //writing dataframe
    df.coalesce(1)
      .write
      .mode("overwrite")
      .parquet(hdfsReportPath)
  }
}
