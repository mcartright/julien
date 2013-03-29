package edu.umass.ciir.julien

sealed trait Support
trait Stored extends Support
trait Synthetic extends Support {
  def supports: Set[Stored]
}
