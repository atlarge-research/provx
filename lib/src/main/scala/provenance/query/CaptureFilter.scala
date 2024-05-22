package lu.magalhaes.gilles.provxlib
package provenance.query

case class CaptureFilter(
    provenanceFilter: ProvenancePredicate,
    dataFilter: DataPredicate
)
