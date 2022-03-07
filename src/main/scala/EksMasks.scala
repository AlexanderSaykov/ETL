object EksMasks extends Masks {

  override val RSD2_1_INCLUDE_CHECK_CORRESP: Seq[String] = Seq("32501", "32502", "45901", "45902", "45903", "45904", "45905", "45906", "45907", "45908", "45909", "45910",
    "45911", "45912", "45913", "45914", "45915", "45916", "45917")
  override val RSD2_1_INCLUDE_CHECK_LINKED: Seq[String] = Seq("91703", "91704")
  override val RSD2_1_CORRESP_EXCLUDE: Seq[String] = Seq("32505", "45918")
  override val RSD2_1_LINKED_EXCLUDE: Seq[String] = Seq("32505", "45918", "45818", "47425", "60324")
  override val RSD2_2_INCLUDE_CHECK_CORRESP_REGEX: Seq[String] = Seq("^47423\\.{8}55")
  override val RSD2_2_CORRESP_INCLUDE: Seq[String] = Seq("445", "446", "447", "448", "449", "450", "451", "452", "453", "454", "455", "456", "457")
  override val RSD2_2_INCLUDE_CHECK_LINKED: Seq[String] = Seq("91703", "91704", "91803")
  override val RSD2_2_LINKED_INCLUDE: Seq[String] = Seq("32505", "45918", "45818", "47425", "60324")
}
