package edu.umass.ciir.julien

trait QueryProcessor {
  protected var _indexes = Set[Index]()
  protected var _models = Set[FeatureOp]()
  var numResults: Int = 100
  def add(i: Index) { _indexes = indexes + i }
  def add(f: FeatureOp*) { _models = f ++: models }
  def indexes: Set[Index] = _indexes
  def models: Set[FeatureOp] = _models

  // The thing that needs implementing in subclasses
  def run: List[ScoredDocument]

  // This probably needs work -- should probably refactor to objects as
  // a "canProcess" check - will help with Processor selection.
  def validated: Boolean = {
    assume(!_models.isEmpty, s"Trying to validate with no models!")

    // Need to verify that all model hooks have attachments.
    // Also will guarantee that if any hooks are attached to an
    // unknown index, it's added to the list of known indexes.
    for (m <- _models) {
      val hooks =
        m.filter(_.isInstanceOf[IndexHook]).map(_.asInstanceOf[IndexHook])

      // Add all previously unknown indexes from attached hooks
      _indexes = hooks.filter(_.isAttached).map(_.attachedIndex) ++: _indexes

      // Conditionally try to hook up if needed
      if (!hooks.forall(_.isAttached) && _indexes.size == 1) {
        hooks.filter(!_.isAttached).foreach(h => h.attach(_indexes.head))
      }

      assume(hooks.forall(_.isAttached),
        "Unable to implicitly attach all hooks to models.")

    }
    return true
  }
}
