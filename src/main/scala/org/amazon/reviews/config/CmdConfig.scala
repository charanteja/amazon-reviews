package org.amazon.reviews.config

import scopt.OParser
import scopt.OParser.sequence

case class CmdConfig(sourceDir: String = "",
                     targetDir: String = "",
                     partitions: Int = 10)
object CmdConfig {

  def parserSpec(): OParser[String, CmdConfig] = {
    val default = CmdConfig()
    val builder = OParser.builder[CmdConfig]
    import builder._
    sequence(
      opt[String]("sourceDir")
        .action((x, c) => c.copy(sourceDir = x))
        .text(s"Location of the source dataset (default: ${default.sourceDir})")
        .optional(),
      opt[String]("targetDir")
        .action((x, c) => c.copy(targetDir = x))
        .text(s"Location of the target dataset(default: ${default.targetDir})")
        .optional(),
      opt[Int]("partitions")
        .action((x, c) => c.copy(partitions = x))
        .text(s"the number of partitions of the produced dataset (default: ${default.partitions})")
        .optional(),
    )
  }
}
