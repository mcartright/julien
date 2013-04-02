package edu.umass.ciir.julien

case class CountStatistics(
  var collFreq: CollFreq,
  var numDocs: NumDocs,
  var collLength: CollLength,
  var docFreq: DocFreq,
  var max: MaximumCount
)
