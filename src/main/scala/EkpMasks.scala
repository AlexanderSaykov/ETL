object EkpMasks extends Masks {

  override val RSD2_1_INCLUDE_CHECK_CORRESP: Seq[String] = Seq("45915", "45917")
  override val RSD2_1_INCLUDE_CHECK_LINKED: Seq[String] = Seq("91704")
  override val RSD2_1_CORRESP_EXCLUDE: Seq[String] = Seq("45918")
  override val RSD2_1_LINKED_EXCLUDE: Seq[String] = Seq("45918", "45818", "47425", "60324")
  override val RSD2_2_INCLUDE_CHECK_CORRESP_REGEX: Seq[String] = Seq()
  override val RSD2_2_CORRESP_INCLUDE: Seq[String] = Seq()
  override val RSD2_2_INCLUDE_CHECK_LINKED: Seq[String] = Seq("91704", "91803")
  override val RSD2_2_LINKED_INCLUDE: Seq[String] = Seq("45918", "47425", "60324")
}
