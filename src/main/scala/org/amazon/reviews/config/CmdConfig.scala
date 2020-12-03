package org.amazon.reviews.config

import scopt.OParser
import scopt.OParser.sequence

case class CmdConfig(source: String = "",
                     target: String = "",
                     partitions: Int = 10)
object CmdConfig {

  def parserSpec(): OParser[String, CmdConfig] = {
    val default = CmdConfig()
    val builder = OParser.builder[CmdConfig]
    import builder._
    sequence(
      opt[String]("source")
        .action((x, c) => c.copy(source = x))
        .text(s"Location of the source dataset (default: ${default.source})")
        .optional(),
      opt[String]("target")
        .action((x, c) => c.copy(target = x))
        .text(s"Location of the target dataset(default: ${default.target})")
        .optional(),
      opt[Int]("partitions")
        .action((x, c) => c.copy(partitions = x))
        .text(s"the number of partitions of the produced dataset (default: ${default.partitions})")
        .optional(),
    )
  }
}
