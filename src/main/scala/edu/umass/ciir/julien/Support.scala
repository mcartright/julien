package edu.umass.ciir.julien

sealed trait Support {
  def supports: Set[Stored]
}

trait Stored extends Support
trait Synthetic extends Support
