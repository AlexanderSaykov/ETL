import org.apache.spark.sql.types._

object SheetSchemas {

  val aggregationSchema: StructType = StructType(List(
    StructField("id_pk", LongType),
    StructField("dt_subject_name", StringType),
    StructField("kt_subject_name", StringType),
    StructField("value_date", TimestampType),
    StructField("dt_account", StringType),
    StructField("dt_account_ek", StringType),
    StructField("kt_account", StringType),
    StructField("kt_account_ek", StringType),
    StructField("dt_account_currency", StringType),
    StructField("kt_account_currency", StringType),
    StructField("dt_amount", DecimalType(17, 2)),
    StructField("kt_amount", DecimalType(17, 2)),
    StructField("dt_is_in_sheet", BooleanType),
    StructField("kt_is_in_sheet", BooleanType),
    StructField("payment_purpose", StringType),
    StructField("docum_id", DecimalType(38, 12)),
    StructField("dt_agreement_id_ek", DecimalType(38, 12)),
    StructField("dt_agreement_number", StringType),
    StructField("dt_agreement_date", DateType),
    StructField("dt_agreement_date_close", DateType),
    StructField("kt_agreement_id_ek", DecimalType(38, 12)),
    StructField("kt_agreement_number", StringType),
    StructField("kt_agreement_date", DateType),
    StructField("kt_agreement_date_close", DateType),
    StructField("document_collection", DecimalType(38, 12)),
    StructField("document_in_fold", DecimalType(38, 12)),
    StructField("is_check", BooleanType),
    StructField("partition_dt", DateType),
    StructField("tax_doc_type_code", StringType),
    StructField("asnu_code_fk", StringType)
  ))


}
