import Const.{EKP_ASNU_CODE, EKS_ASNU_CODE, PARQUET_FORMAT_NAME}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}


class DataSources {

  val spark = SparkSession.builder.master("local").getOrCreate()
  val logger: Logger = LoggerFactory.getLogger(this.getClass)


  def getActionSheet30Df(asnuCode:String):DataFrame={
    logger.info("[AUDIT] reading source table")

    def path(asnuCode: String): String = asnuCode match {
      case EKP_ASNU_CODE => "res/in/upload_sheet_30_1_2_repl_EKP"
      case EKS_ASNU_CODE => "res/in/upload_sheet_30_1_2_repl_EKS"
      case _ => "other"
    }
    spark.read.json(path(asnuCode))
  }

  def getFilialMappingDf():DataFrame={
    logger.info("[AUDIT] reading ess filial map table")

    spark.read.option("multiline", true).json("res/in/ess_filial_map.json")
  }

  def getPaymentPurposeDf():DataFrame={
    logger.info("[AUDIT] reading payment purpose filter table")

    spark.read.option("multiline", true).json("res/in/payment_purpose_filter.json")
  }





}
