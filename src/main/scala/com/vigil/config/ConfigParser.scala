package com.vigil.config

import scopt.{OParser, OParserBuilder}

/**
 * Configuration parser for use as Command Line Interface
 * Example:
 * -- input input --output output --aws-profile profile
 * -i input -o output -a profile
 */
object ConfigParser {
  val builder: OParserBuilder[Config] = OParser.builder[Config]
  val argParser: OParser[Unit, Config] = {
    import builder._
    OParser.sequence(
      programName("scala-engineer-test"),
      opt[String]('i', "input")
        .required()
        .action((b, c) => c.copy(input = b))
        .text("Input path"),
      opt[String]('o', "output")
        .required()
        .action((b, c) => c.copy(output = b))
        .text("Outut path"),
      opt[String]('a', "aws-profile")
        .action((d, c) => c.copy(awsProfile = d))
        .text("Aws profile")
    )
  }
}
