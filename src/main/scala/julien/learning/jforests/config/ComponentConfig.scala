package julien
package learning
package jforests
package config

trait ComponentConfig {
  def toString
  def errorMsg
  def init(config: ConfigHolder)
}
