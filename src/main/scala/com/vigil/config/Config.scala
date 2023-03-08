package com.vigil.config

/**
 * Configuration class with parameters to process
 * @param input s3 input path
 * @param output s3 output path
 * @param awsProfile aws credentials profile
 */
case class Config(
                 input: String = "",
                 output: String = "",
                 awsProfile: String = "default",
                 )
