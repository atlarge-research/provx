package lu.magalhaes.gilles.provxlib
package provenance.query

case class CaptureFilter(
    dataFilter: DataPredicate[_, _] = DataPredicate(),
    provenanceFilter: ProvenancePredicate
)
