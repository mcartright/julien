package julien.learning
package jforests.learning

sealed trait Event

case class IterationEnd extends Event
case class LearningEnd extends Event
case class ScoreSEval extends Event
