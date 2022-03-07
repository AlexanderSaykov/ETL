import java.sql.Timestamp


case class AggregationDTO(
                           id_pk: Int = 1,
                           docum_id: Int = 1,
                           dt_subject_name: String = "1",
                           kt_subject_name: String = "1",
                           value_date: Timestamp = null,
                           payment_purpose: String = null,
                           dt_account: String = null,
                           dt_account_ek: String = null,
                           kt_account: String = null,
                           kt_account_ek: String = null,
                           dt_account_currency: String = null,
                           kt_account_currency: String = null,
                           dt_amount: BigDecimal = null,
                           kt_amount: BigDecimal = null,
                           docum_filial_ek: BigDecimal = 55,
                           docum_filial_dp_code: String = "1",
                           dt_agreement_id_ek: BigDecimal = 1,
                           dt_agreement_number: String = null,
                           dt_agreement_date: Timestamp = null,
                           dt_agreement_date_close: Timestamp = null,
                           dt_account_type: String = null,
                           dt_agreement_type: String = null,
                           kt_agreement_id_ek: BigDecimal = 1,
                           kt_agreement_number: String = "1",
                           kt_agreement_date: Timestamp = Timestamp.valueOf("2021-01-02 00:00:00.000"),
                           kt_agreement_date_close: Timestamp = Timestamp.valueOf("2021-01-02 00:00:00.000"),
                           kt_agreement_type: String = null,
                           kt_account_type: String = null,
                           document_collection: BigDecimal = null,
                           document_in_fold: BigDecimal = null,
                           is_check: Boolean = true,
                           partition_dt: Timestamp = Timestamp.valueOf("2021-01-02 00:00:00.000"),
                           asnu_code_fk: String = null
                         )


case class FilialMapping(
                          usl_div_sk: Long = 55,
                          usl_div_dp_code: String = "1",
                          filial_code_fk: Long = 55,
                          start_date: Timestamp= null,
                          end_date: Timestamp= null
                        )

case class PaymentPurpose(
                          id_pk: Int = 55,
                          asnu_code_fk: String = null,
                          tax_doc_type_code: String = null,
                          use_type: String = null,
                          payment_purpose_pattern: String = null,
                          start_date: String = null,
                          end_date: String = null
                        )
