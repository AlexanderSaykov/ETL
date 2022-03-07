import ApplicationArguments.{END_DATE, START_DATE, SYSTEM_CODE, getOrException}
import com.concurrentthought.cla.Args
import org.apache.spark.sql.SparkSession
import org.mockito.scalatest.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite


class AggregationEKP30Test extends AnyFunSuite with MockitoSugar {

  val spark = SparkSession.builder.master("local").getOrCreate()

  val args: Array[String] = Array(
    "--sc", "6000",
    "-s", "1800-01-01",
    "-e", "9999-12-30",
    "-p", "res/out/ekp"
  )
  val appArgs: Args = ApplicationArguments.initialArgs(args).process(args)
  val startDate: String = getOrException[String](appArgs, START_DATE, required = true)
  val endDate: String = getOrException[String](appArgs, END_DATE, required = true)
  val asnuCode = getOrException[String](appArgs, SYSTEM_CODE, required = true)

  /**
    Here we just run main app. It should write files to res/out/ekp folder
   */

    test("main app test") {
    Application.main(args)
  }


    test("masks kt_account 91704-45918") {

      /**
       * Test checks that transaction where kt_account has mask 91704 and mask 45918
       * passes all filters and aggregations during ETL and appears in final df
       *
       */

      import spark.implicits._

    val dataSources = mock[DataSources]

    when(dataSources.getActionSheet30Df(asnuCode)).thenReturn(
      Seq(
        AggregationDTO(asnu_code_fk = asnuCode,
          dt_account = "45915810602110000011",
          kt_account = "9170481094400010200133",
          document_collection = 1
        ),
        AggregationDTO(asnu_code_fk = asnuCode,
          dt_account = "45915810602110000011",
          kt_account = "4591881094400010200133",
          document_collection = 1
        )
      ).toDF
    )

    when(dataSources.getFilialMappingDf).thenReturn(
      Seq(
        FilialMapping()
      ).toDF
    )

    when(dataSources.getPaymentPurposeDf).thenReturn(
      Seq(
        PaymentPurpose()
      ).toDF
    )


    val df = Sheet30AggregationTransform.transform30_2EKP(
      actionSheetDf = dataSources.getActionSheet30Df(asnuCode),
      filialMapDf = dataSources.getFilialMappingDf,
      paymentPurposeDf = dataSources.getPaymentPurposeDf,
      startDate = startDate,
      endDate = endDate,
      asnuCode = asnuCode,
    )

   assertResult(1)(df.count())
  }

  test("masks dt_account 91704-47425") {

    /**
     * Test checks that transaction where dt_account has mask 91704 and mask 47425
     * passes all filters and aggregations during ETL and appears in final df
     *
     */

    import spark.implicits._

    val dataSources = mock[DataSources]

    when(dataSources.getActionSheet30Df(asnuCode)).thenReturn(
      Seq(
        AggregationDTO(asnu_code_fk = asnuCode,
          dt_account = "91704810044001800133",
          kt_account = "47427810745000385587",
          document_collection = 1
        ),
        AggregationDTO(asnu_code_fk = asnuCode,
          dt_account = "47425810044001800133",
          kt_account = "47427810745000385587",
          document_collection = 1
        )
      ).toDF
    )

    when(dataSources.getFilialMappingDf).thenReturn(
      Seq(
        FilialMapping()
      ).toDF
    )

    when(dataSources.getPaymentPurposeDf).thenReturn(
      Seq(
        PaymentPurpose()
      ).toDF
    )


    val df = Sheet30AggregationTransform.transform30_2EKP(
      actionSheetDf = dataSources.getActionSheet30Df(asnuCode),
      filialMapDf = dataSources.getFilialMappingDf,
      paymentPurposeDf = dataSources.getPaymentPurposeDf,
      startDate = startDate,
      endDate = endDate,
      asnuCode = asnuCode,
    )

    assertResult(1)(df.count())
  }

  test("masks dt_account 91704-60324") {

    /**
     * Test checks that transaction where dt_account has mask 91704 and mask 60324
     * passes all filters and aggregations during ETL and appears in final df
     *
     */

    import spark.implicits._

    val dataSources = mock[DataSources]

    when(dataSources.getActionSheet30Df(asnuCode)).thenReturn(
      Seq(
        AggregationDTO(asnu_code_fk = asnuCode,
          dt_account = "6032481014400020023334",
          kt_account = "60323810444005411728",
          document_collection = 1
        ),
        AggregationDTO(asnu_code_fk = asnuCode,
          dt_account = "9170481014400020023334",
          kt_account = "60323810444005411728",
          document_collection = 1
        )
      ).toDF
    )

    when(dataSources.getFilialMappingDf).thenReturn(
      Seq(
        FilialMapping()
      ).toDF
    )

    when(dataSources.getPaymentPurposeDf).thenReturn(
      Seq(
        PaymentPurpose()
      ).toDF
    )


    val df = Sheet30AggregationTransform.transform30_2EKP(
      actionSheetDf = dataSources.getActionSheet30Df(asnuCode),
      filialMapDf = dataSources.getFilialMappingDf,
      paymentPurposeDf = dataSources.getPaymentPurposeDf,
      startDate = startDate,
      endDate = endDate,
      asnuCode = asnuCode,
    )

    assertResult(1)(df.count())
  }

  test("masks dt_account 91803-45918") {

    /**
     * Test checks that transaction where dt_account has mask 91803 and mask 45918
     * passes all filters and aggregations during ETL and appears in final df
     *
     */
    import spark.implicits._

    val dataSources = mock[DataSources]

    when(dataSources.getActionSheet30Df(asnuCode)).thenReturn(
      Seq(
        AggregationDTO(asnu_code_fk = asnuCode,
          dt_account = "4591881064400010200098",
          kt_account = "45915810645000050426",
          document_collection = 1
        ),
        AggregationDTO(asnu_code_fk = asnuCode,
          dt_account = "918031064400010200098",
          kt_account = "45915810645000050426",
          document_collection = 1
        )
      ).toDF
    )

    when(dataSources.getFilialMappingDf).thenReturn(
      Seq(
        FilialMapping()
      ).toDF
    )

    when(dataSources.getPaymentPurposeDf).thenReturn(
      Seq(
        PaymentPurpose()
      ).toDF
    )


    val df = Sheet30AggregationTransform.transform30_2EKP(
      actionSheetDf = dataSources.getActionSheet30Df(asnuCode),
      filialMapDf = dataSources.getFilialMappingDf,
      paymentPurposeDf = dataSources.getPaymentPurposeDf,
      startDate = startDate,
      endDate = endDate,
      asnuCode = asnuCode,
    )

    assertResult(1)(df.count())
  }

  test("masks dt_account 91803-47425") {

    /**
     * Test checks that transaction where dt_account has mask 91803 and mask 47425
     * passes all filters and aggregations during ETL and appears in final df
     *
     */
    import spark.implicits._

    val dataSources = mock[DataSources]

    when(dataSources.getActionSheet30Df(asnuCode)).thenReturn(
      Seq(
        AggregationDTO(asnu_code_fk = asnuCode,
          dt_account = "91803810244001800111",
          kt_account = "47427810044050506936",
          document_collection = 1
        ),
        AggregationDTO(asnu_code_fk = asnuCode,
          dt_account = "47425810244001800111",
          kt_account = "47427810044050506936",
          document_collection = 1
        )
      ).toDF
    )

    when(dataSources.getFilialMappingDf).thenReturn(
      Seq(
        FilialMapping()
      ).toDF
    )

    when(dataSources.getPaymentPurposeDf).thenReturn(
      Seq(
        PaymentPurpose()
      ).toDF
    )

    val df = Sheet30AggregationTransform.transform30_2EKP(
      actionSheetDf = dataSources.getActionSheet30Df(asnuCode),
      filialMapDf = dataSources.getFilialMappingDf,
      paymentPurposeDf = dataSources.getPaymentPurposeDf,
      startDate = startDate,
      endDate = endDate,
      asnuCode = asnuCode,
    )

    assertResult(1)(df.count())
  }

  test("masks dt_account 91803-60324") {

    /**
     * Test checks that transaction where dt_account has mask 91803 and mask 60324
     * passes all filters and aggregations during ETL and appears in final df
     *
     */
    import spark.implicits._

    val dataSources = mock[DataSources]

    when(dataSources.getActionSheet30Df(asnuCode)).thenReturn(
      Seq(
        AggregationDTO(asnu_code_fk = asnuCode,
          dt_account = "6032481024400020040548",
          kt_account = "60323810144005319751",
          document_collection = 1
        ),
        AggregationDTO(asnu_code_fk = asnuCode,
          dt_account = "9180381024400020040548",
          kt_account = "47427810044050506936",
          document_collection = 1
        )
      ).toDF
    )

    when(dataSources.getFilialMappingDf).thenReturn(
      Seq(
        FilialMapping()
      ).toDF
    )

    when(dataSources.getPaymentPurposeDf).thenReturn(
      Seq(
        PaymentPurpose()
      ).toDF
    )


    val df = Sheet30AggregationTransform.transform30_2EKP(
      actionSheetDf = dataSources.getActionSheet30Df(asnuCode),
      filialMapDf = dataSources.getFilialMappingDf,
      paymentPurposeDf = dataSources.getPaymentPurposeDf,
      startDate = startDate,
      endDate = endDate,
      asnuCode = asnuCode,
    )

    assertResult(1)(df.count())
  }

  test("payment_purpose filter") {

    /**
     * Test checks that payment_purpose filter works fine
     *
     */
    import spark.implicits._

    val dataSources = mock[DataSources]

    when(dataSources.getActionSheet30Df(asnuCode)).thenReturn(
      Seq(
        AggregationDTO(asnu_code_fk = asnuCode,
          dt_account = "111",
          kt_account = "91704810000020200834",
          document_collection = 1,
          payment_purpose = "credit payment 1q"
        )
      ).toDF
    )

    when(dataSources.getFilialMappingDf).thenReturn(
      Seq(
        FilialMapping()
      ).toDF
    )
    when(dataSources.getPaymentPurposeDf).thenReturn(
      Seq(
        PaymentPurpose(use_type = "INCLUDE",
          payment_purpose_pattern = "credit payment 1q",
          tax_doc_type_code = "sheet_302",
          asnu_code_fk = asnuCode)
      ).toDF
    )

    val df = Sheet30AggregationTransform.transform30_2EKP(
      actionSheetDf = dataSources.getActionSheet30Df(asnuCode),
      filialMapDf = dataSources.getFilialMappingDf,
      paymentPurposeDf = dataSources.getPaymentPurposeDf,
      startDate = startDate,
      endDate = endDate,
      asnuCode = asnuCode,
    )

   assertResult(1)(df.count())
  }


  test("kt columns not empty") {

    /**
     * Test checks that kt_subject_name, kt_agreement_id_ek, kt_agreement_number
     * kt_agreement_date and kt_agreement_date_close columns are not empty
     *
     */

    import spark.implicits._

    val dataSources = mock[DataSources]

    when(dataSources.getActionSheet30Df(asnuCode)).thenReturn(
      Seq(
        AggregationDTO(asnu_code_fk = asnuCode,
          dt_account = "47425810044001800133",
          kt_account = "45915810645000050426",
          document_collection = 1
        ),
        AggregationDTO(asnu_code_fk = asnuCode,
          dt_account = "91803810044001800133",
          kt_account = "45915810645000050426",
          document_collection = 1
        )
      ).toDF
    )

    when(dataSources.getFilialMappingDf).thenReturn(
      Seq(
        FilialMapping()
      ).toDF
    )

    when(dataSources.getPaymentPurposeDf).thenReturn(
      Seq(
        PaymentPurpose()
      ).toDF
    )


    val df = Sheet30AggregationTransform.transform30_2EKP(
      actionSheetDf = dataSources.getActionSheet30Df(asnuCode),
      filialMapDf = dataSources.getFilialMappingDf,
      paymentPurposeDf = dataSources.getPaymentPurposeDf,
      startDate = startDate,
      endDate = endDate,
      asnuCode = asnuCode,
    )

   assertResult(1)(df.select("kt_subject_name").count())
    assertResult(1)(df.select("kt_agreement_id_ek").count())
    assertResult(1)(df.select("kt_agreement_number").count())
    assertResult(1)(df.select("kt_agreement_date").count())
    assertResult(1)(df.select("kt_agreement_date_close").count())
  }
}
