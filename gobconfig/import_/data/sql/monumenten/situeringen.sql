SELECT identificatie,
       "hoortBijMonumentenMonument" AS hoort_bij_monumenten_monument,
       "betreftBagNummeraanduiding" AS betreft_bag_nummeraanduiding,
       "eersteSituering"            AS eerste_situering,
       "situeringNummeraanduiding"  AS situering_nummeraanduiding,
       "datumActueelTot"            AS datum_actueel_tot
FROM monumenten_prepared.situeringen
