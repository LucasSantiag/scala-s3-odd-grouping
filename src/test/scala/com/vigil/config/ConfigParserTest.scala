package com.vigil.config

import org.scalatest.funsuite.AnyFunSuite
import scopt.OParser

class ConfigParserTest extends AnyFunSuite {
  lazy val input = "input"
  lazy val output = "output"
  lazy val awsProfile = "profile"

  test("input all variables should retrieve config class") {
    val config = OParser.parse(ConfigParser.argParser, Array("--input", input, "--output", output, "--aws-profile", awsProfile), Config())
    assert(config.isDefined)
    assert(config.get.input == input)
    assert(config.get.output == output)
    assert(config.get.awsProfile == awsProfile)
  }

  test("input with only required variables (input and output) should retrieve config class") {
    val config = OParser.parse(ConfigParser.argParser, Array("--input", input, "--output", output), Config())
    assert(config.isDefined)
    assert(config.get.input == input)
    assert(config.get.output == output)
    assert(config.get.awsProfile == "default")
  }

  test("input without required variables (without input) should fail config class setup") {
    val config = OParser.parse(ConfigParser.argParser, Array("--output", output), Config())
    assert(config.isEmpty)
  }

  test("input without required variables (without output) should fail config class setup") {
    val config = OParser.parse(ConfigParser.argParser, Array("--input", input), Config())
    assert(config.isEmpty)
  }
}
