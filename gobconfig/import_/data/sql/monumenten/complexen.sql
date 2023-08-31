SELECT identificatie,
       "monumentnummerComplex"                 as monumentnummer_complex,
       "complexNaam"                           as complex_naam,
       "bestaatUitMonumentenMonumenten"::jsonb as bestaat_uit_monumenten_monumenten,
       beschrijving,
       "beschrijvingPubliek"                   as beschrijving_publiek
FROM monumenten_prepared.complexen
