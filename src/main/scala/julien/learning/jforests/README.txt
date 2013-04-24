This is an adaptation of the JForests library by
Yasser Ganjisaffar <ganjisaffar at gmail dot com>.

Several of the packages from JForests are not being ported,
since they were deemed unnecessary in Scala (the language
provides the necessary constructs), or we have implementations
on site already that we would like to unify with this port.
The elided packages/classes are:

util/concurrency/*
util/Timer.java
applications/*
eval/ranking/{MAPEval, NDCGEval, PrecisionEval}
input/sparse/*
input/*
learning/LearningUtils (methods reallocated to the modules where they are
		       	called, or are simply inlined)
learning/LearningProgressListener (replaced by Publisher/Subscriber)

Some classes and/or definitions have been moved into package objects
for ease of use. These are:

util/MathUtil.java (pending)
util/Constants.java
learning/trees/Ensemble (replaced type alias + object)

Notes
-----

- RegressionTree is MUCH smaller than it's original, b/c of what appeared
to be an orphaned method (see "normalizeNodeNames" in the original)
