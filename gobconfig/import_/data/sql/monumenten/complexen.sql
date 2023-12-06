SELECT identificatie,
       "monumentnummerComplex"                 AS monumentnummer_complex,
       naam,
       "bestaatUitMonumentenMonumenten"::jsonb AS bestaat_uit_monumenten_monumenten,
       beschrijving,
       "beschrijvingPubliek"                   AS beschrijving_publiek,
       "datumActueelTot"                       AS datum_actueel_tot
FROM monumenten_prepared.complexen
