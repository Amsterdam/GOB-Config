SELECT identificatie,
       "hoortBijMonumentenMonument" as hoort_bij_monumenten_monument,
       "betreftBagNummeraanduiding" as betreft_bag_nummeraanduiding,
       "eersteSituering"            as eerste_situering,
       "situeringNummeraanduiding"  as situering_nummeraanduiding
FROM monumenten_prepared.situeringen
