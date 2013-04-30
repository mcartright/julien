package julien

import java.io.PrintStream;
import julien.galago.tupleflow.Parameters;
import julien.galago.tupleflow.Utility;
import julien.galago.tupleflow.execution.ErrorStore;
import julien.galago.tupleflow.execution.Job;
import julien.galago.tupleflow.execution.JobExecutor;

trait TupleFlowFunction extends CLIFunction {
  def runTupleFlowJob(job: Job, p: Parameters, out: PrintStream) : Boolean = {
    if (p.isBoolean("printJob") && p.getBoolean("printJob")) {
      p.remove("printJob")
      p.set("printJob", "dot")
    }

    val printJob = p.get("printJob", "none")
    if (printJob.equals("plan")) {
      out.println(job.toString())
      return true
    } else if (printJob.equals("dot")) {
      out.println(job.toDotString())
      return true
    }

    val hash = p.get("distrib", 0).toInt
    if (hash > 0) {
      job.properties.put("hashCount", Integer.toString(hash))
    }

    val store = new ErrorStore()
    JobExecutor.runLocally(job, store, p)
    if (store.hasStatements()) {
      out.println(store.toString())
      return false
    }
    return true
  }

  def tupleflowHelp : String =
"""Tupleflow Flags:
  --printJob={true|false}: Simply prints the execution plan of a Tupleflow-based job then exits.
                           [default=false]
  --mode={local|threaded|drmaa}: Selects which executor to use
                           [default=local]
  --port={int<65000} :     port number for web based progress monitoring.
                           [default=randomly selected free port]
  --galagoJobDir=/path/to/temp/dir/: Sets the galago temp dir
                           [default = specified in ~/.galagotmp or java.io.tmpdir]
  --deleteJobDir={true|false}: Selects to delete the galago job directory
                           [default = true]
  --distrib={int > 1}:     Selects the number of simultaneous jobs to create
                           [default = 10]
  --server={true|false}:   Selects to use a server to show the progress of a tupleflow execution.
                           [default = true]
"""
}
