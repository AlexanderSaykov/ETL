import ApplicationArguments.initialArgs
import com.concurrentthought.cla.{Args, Opt}

class ApplicationArguments(val args: Array[String]) {
  val parsedInitialArgs: Args = initialArgs(args).parse(args)
}

object ApplicationArguments {

  val RNU_NAME: String = "rnu_name"
  val rnu_name: Opt[String] = Opt.string(
    name = RNU_NAME,
    flags = Seq("-r", "--rnu", "--rnu_name"),
    help = "Name of RNU report (RNU4, RNU5, SHEET22, etc.)",
    required = false)

  val START_DATE: String = "start_date"
  val start_date: Opt[String] = Opt.string(
    name = START_DATE,
    flags = Seq("-s", "--start", "--start_date"),
    help = "Period start date",
    required = false)

  val END_DATE: String = "end_date"
  val end_date: Opt[String] = Opt.string(
    name = END_DATE,
    flags =
      Seq("-e", "--end", "--end_date"),
    help = "Period end date",
    required = false)

  val SYSTEM_CODE: String = "system_code"
  val system_code: Opt[String] = Opt.string(
    name = SYSTEM_CODE,
    flags = Seq("--sc", "--system_code"),
    help = "code of system ASNU",
    required = false)


  val HDFS_PATH: String = "write_path"
  val hdfsPath: Opt[String] = Opt.string(
    name = HDFS_PATH,
    flags = Seq("-p", "--path"),
    help = "Path to write dataframe",
    required = false)

  def initialArgs(argstrings: Seq[String]): Args =
    Args(Seq(
      rnu_name,
      start_date,
      end_date,
      system_code,
      hdfsPath,
      Args.quietFlag)).parse(argstrings)

  /** Get argument of application */
  def getOrException[T](appArgs: Args, key: String, required: Boolean): T = {
    val x: Option[T] = appArgs.get(key)
    x match {
      case Some(value) => value
      case None => if (required) throw new IllegalArgumentException(s"$key argument is empty or incorrect") else x.getOrElse(null.asInstanceOf[T])
    }
  }

}
