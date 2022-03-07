import Const.{DATE_FMT_PATTERN, EKP_ASNU_CODE, EKS_ASNU_CODE, RSD2_2_INCLUDE_CHECK_CORRESP, SHEET_30_2_CODE}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, substring, when, _}

object Sheet30AggregationTransform {

  def transform(actionSheetDf: DataFrame,
                filialMapDf: DataFrame,
                paymentPurposeDf: DataFrame,
                startDate: String,
                endDate: String,
                asnuCode: String
               ): DataFrame = {
    asnuCode match {
      case EKP_ASNU_CODE => transform30_2EKP(actionSheetDf, filialMapDf, paymentPurposeDf, startDate, endDate, asnuCode)
      case EKS_ASNU_CODE => transform30_2EKS(actionSheetDf, filialMapDf, paymentPurposeDf, startDate, endDate, asnuCode)
      case _ => throw new IllegalArgumentException(s"Unexpected asnu code $asnuCode")
    }

  }

  def transform30_2EKS(actionSheetDf: DataFrame,
                       filialMapDf: DataFrame,
                       paymentPurposeDf: DataFrame,
                       startDate: String,
                       endDate: String,
                       asnuCode: String
                      ): DataFrame = {

    val masks = asnuCode match {
      case EKP_ASNU_CODE => EkpMasks
      case EKS_ASNU_CODE => EksMasks
      case _ => throw new IllegalStateException(s"Unknown asnu code $asnuCode")
    }


    val paymentPurposesCond = generatePaymentCondition(
      paymentPurposeDf,
      asnuCode = asnuCode,
      taxDocCode = SHEET_30_2_CODE,
      useType = "INCLUDE"
    )

    val conditionIncludecheckLinked: Column = substring(col("kt_account"), 1, 5).isin(masks.RSD2_2_INCLUDE_CHECK_LINKED: _*) ||
      substring(col("dt_account"), 1, 5).isin(masks.RSD2_2_INCLUDE_CHECK_LINKED: _*)

    val conditionLinkedInclude: Column = substring(col("kt_account"), 1, 5).isin(masks.RSD2_2_LINKED_INCLUDE: _*) ||
      substring(col("dt_account"), 1, 5).isin(masks.RSD2_2_LINKED_INCLUDE: _*)



    val baseDf1 = actionSheetDf.filter(col("asnu_code_fk") === asnuCode)
      .filter(col("partition_dt").between(to_timestamp(lit(startDate), DATE_FMT_PATTERN),
        to_timestamp(lit(endDate), DATE_FMT_PATTERN)))
      .filter(col("document_collection").isNotNull)
    baseDf1.cache()
    baseDf1.count()

    val baseDfA1 = baseDf1.filter((col("kt_account").like(RSD2_2_INCLUDE_CHECK_CORRESP) &&
      substring(col("dt_account"), 1, 3).isin(masks.RSD2_2_CORRESP_INCLUDE: _*)) ||
      (col("dt_account").like(RSD2_2_INCLUDE_CHECK_CORRESP) &&
        substring(col("kt_account"), 1, 3).isin(masks.RSD2_2_CORRESP_INCLUDE: _*)))
      .withColumn("dt_is_in_sheet", when(col("dt_account").like(RSD2_2_INCLUDE_CHECK_CORRESP), true).otherwise(false))
      .withColumn("kt_is_in_sheet", when(col("kt_account").like(RSD2_2_INCLUDE_CHECK_CORRESP), true).otherwise(false))

    val baseDfAMainKtDt = baseDf1.filter(col("dt_subject_name").isNotNull || col("kt_subject_name").isNotNull)
      .groupBy("document_collection").max("dt_agreement_id_ek", "kt_agreement_id_ek")
      .withColumnRenamed("max(dt_agreement_id_ek)", "dt_agreement_id_ek")
      .withColumnRenamed("max(kt_agreement_id_ek)", "kt_agreement_id_ek")


    val baseDfAMainDt = baseDfAMainKtDt.join(baseDf1.alias("baseDf"),
      Seq("document_collection", "dt_agreement_id_ek"), "inner")
      .select(col("document_collection"),
        col("dt_agreement_id_ek"),
        col("baseDf.dt_agreement_number"),
        col("baseDf.dt_agreement_date"),
        col("baseDf.dt_agreement_date_close"),
        col("baseDf.dt_subject_name"),
        col("baseDf.partition_dt"))
      .withColumnRenamed("dt_agreement_id_ek", "agreement_id_ek")
      .withColumnRenamed("dt_agreement_number", "agreement_number")
      .withColumnRenamed("dt_agreement_date", "agreement_date")
      .withColumnRenamed("dt_agreement_date_close", "agreement_date_close")
      .withColumnRenamed("dt_subject_name", "subject_name")


    val baseDfAMainKt = baseDfAMainKtDt.join(baseDf1.alias("baseDf"),
      Seq("document_collection", "kt_agreement_id_ek"), "inner")
      .select(col("document_collection"),
        col("kt_agreement_id_ek"),
        col("baseDf.kt_agreement_number"),
        col("baseDf.kt_agreement_date"),
        col("baseDf.kt_agreement_date_close"),
        col("baseDf.kt_subject_name"),
        col("baseDf.partition_dt"))
      .withColumnRenamed("kt_agreement_id_ek", "agreement_id_ek")
      .withColumnRenamed("kt_agreement_number", "agreement_number")
      .withColumnRenamed("kt_agreement_date", "agreement_date")
      .withColumnRenamed("kt_agreement_date_close", "agreement_date_close")
      .withColumnRenamed("kt_subject_name", "subject_name")


    val baseDfAMainDtKtUnion = baseDfAMainDt.union(baseDfAMainKt)

    val baseDfBCorr = baseDf1.filter(conditionLinkedInclude)

    val baseDfB1 = baseDfAMainDtKtUnion.alias("baseDfBMain").join(baseDfBCorr.alias("baseDfBCorr"), Seq("document_collection", "partition_dt"), "inner")
      .select(col("baseDfBCorr.id_pk"),
        col("baseDfBCorr.docum_id"),
        col("baseDfBMain.subject_name").as("dt_subject_name"),
        col("baseDfBMain.subject_name").as("kt_subject_name"),
        col("baseDfBCorr.value_date"),
        col("baseDfBCorr.dt_account"),
        col("baseDfBCorr.dt_account_ek"),
        col("baseDfBCorr.kt_account"),
        col("baseDfBCorr.kt_account_ek"),
        col("baseDfBCorr.dt_account_currency"),
        col("baseDfBCorr.kt_account_currency"),
        col("baseDfBCorr.dt_amount"),
        col("baseDfBCorr.kt_amount"),
        col("baseDfBCorr.payment_purpose"),
        col("baseDfBCorr.docum_filial_ek"),
        col("baseDfBCorr.docum_filial_dp_code"),
        col("baseDfBMain.agreement_id_ek").as("dt_agreement_id_ek"),
        col("baseDfBMain.agreement_id_ek").as("kt_agreement_id_ek"),
        col("baseDfBMain.agreement_number").as("dt_agreement_number"),
        col("baseDfBMain.agreement_number").as("kt_agreement_number"),
        col("baseDfBMain.agreement_date").as("dt_agreement_date"),
        col("baseDfBMain.agreement_date").as("kt_agreement_date"),
        col("baseDfBMain.agreement_date_close").as("dt_agreement_date_close"),
        col("baseDfBMain.agreement_date_close").as("kt_agreement_date_close"),
        col("baseDfBCorr.kt_agreement_type"),
        col("baseDfBCorr.dt_agreement_type"),
        col("baseDfBCorr.kt_account_type"),
        col("baseDfBCorr.dt_account_type"),
        col("document_collection"),
        col("baseDfBCorr.document_in_fold"),
        col("baseDfBCorr.asnu_code_fk"),
        col("partition_dt"),
        col("baseDfBCorr.is_check"))
      .withColumn("dt_is_in_sheet", when(substring(col("dt_account"), 1, 5).isin(masks.RSD2_2_LINKED_INCLUDE: _*), false).otherwise(true))
      .withColumn("kt_is_in_sheet", when(substring(col("kt_account"), 1, 5).isin(masks.RSD2_2_LINKED_INCLUDE: _*), false).otherwise(true))
      .dropDuplicates("docum_id")

    val baseDfCMain = baseDf1.filter(conditionIncludecheckLinked)

    val baseDfCCorr = baseDf1.filter(conditionIncludecheckLinked).filter(col("is_check") === true)
      .select("document_collection", "partition_dt")

    val baseDfC1 = baseDfCMain.join(baseDfCCorr, baseDfCMain("document_collection") =!= baseDfCCorr("document_collection") &&
      baseDfCMain("partition_dt") =!= baseDfCCorr("partition_dt"), "left")
      .drop(baseDfCCorr("document_collection"))
      .drop(baseDfCCorr("partition_dt"))
      .filter(paymentPurposesCond)
      .withColumn("dt_is_in_sheet", when(substring(col("dt_account"), 1, 5).isin(masks.RSD2_2_INCLUDE_CHECK_LINKED: _*), true).otherwise(false))
      .withColumn("kt_is_in_sheet", when(substring(col("kt_account"), 1, 5).isin(masks.RSD2_2_INCLUDE_CHECK_LINKED: _*), true).otherwise(false))




    val baseDf2 = actionSheetDf.filter(col("asnu_code_fk") === asnuCode)
      .filter(col("partition_dt").between(to_timestamp(lit(startDate), DATE_FMT_PATTERN),
        to_timestamp(lit(endDate), DATE_FMT_PATTERN)))
      .filter(col("document_collection").isNull)

    baseDf2.cache()
    baseDf2.count()


    val baseDfA2 = baseDf2.filter((col("kt_account").like(RSD2_2_INCLUDE_CHECK_CORRESP) &&
      substring(col("dt_account"), 1, 3).isin(masks.RSD2_2_CORRESP_INCLUDE: _*)) ||
      (col("dt_account").like(RSD2_2_INCLUDE_CHECK_CORRESP) &&
        substring(col("kt_account"), 1, 3).isin(masks.RSD2_2_CORRESP_INCLUDE: _*)))
      .withColumn("dt_is_in_sheet", when(col("dt_account").like(RSD2_2_INCLUDE_CHECK_CORRESP), true).otherwise(false))
      .withColumn("kt_is_in_sheet", when(col("kt_account").like(RSD2_2_INCLUDE_CHECK_CORRESP), true).otherwise(false))

    val baseDfC2Main = baseDf2.filter(conditionIncludecheckLinked)


    val baseDfC2Corr = baseDf2.filter(col("is_check") === true)
      .filter(conditionLinkedInclude)
      .select("document_collection", "partition_dt")

    val baseDfC2 = baseDfC2Main.join(baseDfC2Corr, baseDfC2Main("document_collection") =!= baseDfC2Corr("document_collection") &&
      baseDfC2Main("partition_dt") =!= baseDfC2Corr("partition_dt"), "left")
      .drop(baseDfC2Corr("document_collection"))
      .drop(baseDfC2Corr("partition_dt"))
      .filter(paymentPurposesCond)
      .withColumn("dt_is_in_sheet", when(substring(col("dt_account"), 1, 5).isin(masks.RSD2_2_INCLUDE_CHECK_LINKED: _*), true).otherwise(false))
      .withColumn("kt_is_in_sheet", when(substring(col("kt_account"), 1, 5).isin(masks.RSD2_2_INCLUDE_CHECK_LINKED: _*), true).otherwise(false))


    val report = baseDfA1.union(baseDfB1.select(baseDfA1.columns.head, baseDfA1.columns.tail: _*))
      .union(baseDfC1.select(baseDfA1.columns.head, baseDfA1.columns.tail: _*))
      .union(baseDfA2.select(baseDfA1.columns.head, baseDfA1.columns.tail: _*))
      .union(baseDfC2.select(baseDfA1.columns.head, baseDfA1.columns.tail: _*))
      .filter(
        (col("dt_is_in_sheet") && col("dt_agreement_id_ek").isNotNull)
          || (col("kt_is_in_sheet") && col("kt_agreement_id_ek").isNotNull)
      )
      .join(filialMapDf.alias("filial_map"),
        col("docum_filial_ek") === filialMapDf("usl_div_sk")
          && filialMapDf("usl_div_dp_code") === baseDfA1("docum_filial_dp_code")
          && baseDfA1("partition_dt").between(filialMapDf("start_date"), filialMapDf("end_date")),
        "left"
      )
      .withColumn("filial_code", filialMapDf("filial_code_fk"))
      .withColumn("asnu_code_fk", lit(asnuCode))
      .withColumn("tax_doc_type_code", lit(SHEET_30_2_CODE))
      .select(SheetSchemas.aggregationSchema.map(it => col(it.name).cast(it.dataType)): _*)

    report
  }

  def transform30_2EKP(actionSheetDf: DataFrame,
                       filialMapDf: DataFrame,
                       paymentPurposeDf: DataFrame,
                       startDate: String,
                       endDate: String,
                       asnuCode: String
                      ): DataFrame = {

    val masks = asnuCode match {
      case EKP_ASNU_CODE => EkpMasks
      case EKS_ASNU_CODE => EksMasks
      case _ => throw new IllegalStateException(s"Unknown asnu code $asnuCode")
    }

    val paymentPurposesCond = generatePaymentCondition(
      paymentPurposeDf,
      asnuCode = asnuCode,
      taxDocCode = SHEET_30_2_CODE,
      useType = "INCLUDE"
    )

    val baseDf = actionSheetDf
      .filter(col("asnu_code_fk") === asnuCode)
      .filter(col("partition_dt").between(
        to_timestamp(lit(startDate), DATE_FMT_PATTERN),
        to_timestamp(lit(endDate), DATE_FMT_PATTERN)))

    baseDf.cache()
    baseDf.count()

    val conditionIncludeCheckLinked: Column = substring(col("kt_account"), 1, 5).isin(masks.RSD2_2_INCLUDE_CHECK_LINKED: _*) ||
      substring(col("dt_account"), 1, 5).isin(masks.RSD2_2_INCLUDE_CHECK_LINKED: _*)

    val baseDfAMain = baseDf.filter(conditionIncludeCheckLinked)

    val baseDfAMainKtDt = baseDfAMain
      .filter(col("dt_subject_name").isNotNull || col("kt_subject_name").isNotNull)
      .groupBy("document_collection").max("dt_agreement_id_ek", "kt_agreement_id_ek")
      .withColumnRenamed("max(dt_agreement_id_ek)", "dt_agreement_id_ek")
      .withColumnRenamed("max(kt_agreement_id_ek)", "kt_agreement_id_ek")


    val baseDfAMainDt = baseDfAMainKtDt.join(baseDf.alias("baseDf"),
      Seq("document_collection", "dt_agreement_id_ek"), "inner")
      .select(col("document_collection"),
        col("dt_agreement_id_ek"),
        col("baseDf.dt_agreement_number"),
        col("baseDf.dt_agreement_date"),
        col("baseDf.dt_agreement_date_close"),
        col("baseDf.dt_subject_name"),
        col("baseDf.partition_dt"))
      .withColumnRenamed("dt_agreement_id_ek", "agreement_id_ek")
      .withColumnRenamed("dt_agreement_number", "agreement_number")
      .withColumnRenamed("dt_agreement_date", "agreement_date")
      .withColumnRenamed("dt_agreement_date_close", "agreement_date_close")
      .withColumnRenamed("dt_subject_name", "subject_name")


    val baseDfAMainKt = baseDfAMainKtDt.join(baseDf.alias("baseDf"),
      Seq("document_collection", "kt_agreement_id_ek"), "inner")
      .select(col("document_collection"),
        col("kt_agreement_id_ek"),
        col("baseDf.kt_agreement_number"),
        col("baseDf.kt_agreement_date"),
        col("baseDf.kt_agreement_date_close"),
        col("baseDf.kt_subject_name"),
        col("baseDf.partition_dt"))
      .withColumnRenamed("kt_agreement_id_ek", "agreement_id_ek")
      .withColumnRenamed("kt_agreement_number", "agreement_number")
      .withColumnRenamed("kt_agreement_date", "agreement_date")
      .withColumnRenamed("kt_agreement_date_close", "agreement_date_close")
      .withColumnRenamed("kt_subject_name", "subject_name")

    val baseDfAMainDtKtUnion = baseDfAMainDt.union(baseDfAMainKt)


    val baseDfACorr = baseDf.filter(substring(col("kt_account"), 1, 5).isin(masks.RSD2_2_LINKED_INCLUDE: _*) ||
      substring(col("dt_account"), 1, 5).isin(masks.RSD2_2_LINKED_INCLUDE: _*))


    val baseDfA = baseDfAMainDtKtUnion.alias("baseDfAMain")
      .join(baseDfACorr.alias("baseDfACorr"), Seq("document_collection", "partition_dt"), "inner")
      .select(col("baseDfACorr.id_pk"),
        col("baseDfACorr.docum_id"),
        col("baseDfAMain.subject_name").as("dt_subject_name"),
        col("baseDfAMain.subject_name").as("kt_subject_name"),
        col("baseDfACorr.value_date"),
        col("baseDfACorr.dt_account"),
        col("baseDfACorr.dt_account_ek"),
        col("baseDfACorr.kt_account"),
        col("baseDfACorr.kt_account_ek"),
        col("baseDfACorr.dt_account_currency"),
        col("baseDfACorr.kt_account_currency"),
        col("baseDfACorr.dt_amount"),
        col("baseDfACorr.kt_amount"),
        col("baseDfACorr.payment_purpose"),
        col("baseDfACorr.docum_filial_ek"),
        col("baseDfACorr.docum_filial_dp_code"),
        col("baseDfAMain.agreement_id_ek").as("dt_agreement_id_ek"),
        col("baseDfAMain.agreement_id_ek").as("kt_agreement_id_ek"),
        col("baseDfAMain.agreement_number").as("dt_agreement_number"),
        col("baseDfAMain.agreement_number").as("kt_agreement_number"),
        col("baseDfAMain.agreement_date").as("dt_agreement_date"),
        col("baseDfAMain.agreement_date").as("kt_agreement_date"),
        col("baseDfAMain.agreement_date_close").as("dt_agreement_date_close"),
        col("baseDfAMain.agreement_date_close").as("kt_agreement_date_close"),
        col("baseDfACorr.kt_agreement_type"),
        col("baseDfACorr.dt_agreement_type"),
        col("baseDfACorr.kt_account_type"),
        col("baseDfACorr.dt_account_type"),
        col("document_collection"),
        col("baseDfACorr.document_in_fold"),
        col("baseDfACorr.asnu_code_fk"),
        col("partition_dt"),
        col("baseDfACorr.is_check"))
      .withColumn("dt_is_in_sheet", when(substring(col("dt_account"), 1, 5).isin(masks.RSD2_2_LINKED_INCLUDE: _*), false).otherwise(true))
      .withColumn("kt_is_in_sheet", when(substring(col("kt_account"), 1, 5).isin(masks.RSD2_2_LINKED_INCLUDE: _*), false).otherwise(true))
      .dropDuplicates("docum_id")

    val baseDfBMain = baseDf.filter(conditionIncludeCheckLinked).filter(paymentPurposesCond)

    val baseDfBCorr = baseDf.filter(!substring(col("kt_account"), 1, 5).isin(masks.RSD2_2_LINKED_INCLUDE: _*) ||
      !substring(col("dt_account"), 1, 5).isin(masks.RSD2_2_LINKED_INCLUDE: _*))
      .select("document_collection", "partition_dt")

    val baseDfB = baseDfBMain.alias("baseDfBMain").join(baseDfBCorr.alias("baseDfBCorr"), Seq("document_collection", "partition_dt"), "inner")
      .select("baseDfBMain.id_pk",
        "baseDfBMain.docum_id",
        "baseDfBMain.dt_subject_name",
        "baseDfBMain.kt_subject_name",
        "baseDfBMain.value_date",
        "baseDfBMain.dt_account",
        "baseDfBMain.dt_account_ek",
        "baseDfBMain.kt_account",
        "baseDfBMain.kt_account_ek",
        "baseDfBMain.dt_account_currency",
        "baseDfBMain.kt_account_currency",
        "baseDfBMain.dt_amount",
        "baseDfBMain.kt_amount",
        "baseDfBMain.payment_purpose",
        "baseDfBMain.docum_filial_ek",
        "baseDfBMain.docum_filial_dp_code",
        "baseDfBMain.dt_agreement_id_ek",
        "baseDfBMain.kt_agreement_id_ek",
        "baseDfBMain.dt_agreement_number",
        "baseDfBMain.kt_agreement_number",
        "baseDfBMain.dt_agreement_date",
        "baseDfBMain.kt_agreement_date",
        "baseDfBMain.dt_agreement_date_close",
        "baseDfBMain.kt_agreement_date_close",
        "baseDfBMain.dt_agreement_type",
        "baseDfBMain.kt_agreement_type",
        "baseDfBMain.dt_account_type",
        "baseDfBMain.kt_account_type",
        "document_collection",
        "baseDfBMain.document_in_fold",
        "baseDfBMain.asnu_code_fk",
        "partition_dt",
        "baseDfBMain.is_check")
      .withColumn("dt_is_in_sheet", when(substring(col("dt_account"), 1, 5).isin(masks.RSD2_2_INCLUDE_CHECK_LINKED: _*), true).otherwise(false))
      .withColumn("kt_is_in_sheet", when(substring(col("kt_account"), 1, 5).isin(masks.RSD2_2_INCLUDE_CHECK_LINKED: _*), true).otherwise(false))
      .dropDuplicates("docum_id")

    val report = baseDfA.union(baseDfB.select(baseDfA.columns.head, baseDfA.columns.tail: _*))
      .join(filialMapDf.alias("filial_map"),
        col("docum_filial_ek") === filialMapDf("usl_div_sk")
          && filialMapDf("usl_div_dp_code") === baseDfA("docum_filial_dp_code")
          && baseDfA("partition_dt").between(filialMapDf("start_date"), filialMapDf("end_date")),
        "left"
      )
      .withColumn("filial_code", filialMapDf("filial_code_fk"))
      .withColumn("asnu_code_fk", lit(asnuCode))
      .withColumn("tax_doc_type_code", lit(SHEET_30_2_CODE))
      .select(SheetSchemas.aggregationSchema.map(it => col(it.name).cast(it.dataType)): _*)

    report
  }

       private def generatePaymentCondition(paymentPurposeDf: DataFrame, asnuCode: String, taxDocCode: String, useType: String): Column = {
    paymentPurposeDf
      .filter(col("asnu_code_fk") === asnuCode)
      .filter(col("tax_doc_type_code") === taxDocCode)
      .filter(col("use_type") === useType)
      .select("payment_purpose_pattern")
      .distinct()
      .collect()
      .map('%' + _.getString(0) + '%')
      .foldRight(lit(false))(col("payment_purpose").like(_) || _)
  }
}

