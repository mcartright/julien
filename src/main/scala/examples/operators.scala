// SDM fully expanded (using re-used variables)
val whatever = Set[String]("new", "york", "city")x
val query = "new york city".split(" ").map(Term(_))

val sdm =
  Combine(
    Weight(Combine(query.map(a => Dirichlet(a))), 0.8),
    Weight(Combine(query.sliding(2,1).map ( p => OD(p, 1) )), 0.15),
    Weight(Combine(query.sliding(2,1).map ( p => UW(p, 8) )), 0.05)
  )

val weirder =
  Combine(
    Weight(Combine(query.map(a => Dirichlet(a))), 0.5),
    Require((d: Document) =>
      (d.title.split(" ").toSet. & whatever).size > 0,
      Weight(MyOperator(query), 3.5))
  )
