SELECT t.identificatie
     , t.object_identificatie
     , t.versie_identificatie
     , CASE t.objecttype
           WHEN 102 THEN 'Verblijfsobject'
           WHEN 111 THEN 'Ligplaats'
           WHEN 112 THEN 'Standplaats'
           WHEN 101 THEN 'Pand'
           WHEN 21 THEN 'Nummeraanduiding'
           WHEN 20 THEN 'Openbare ruimte'
           WHEN 113 THEN 'Woonplaats'
    END                                                                 AS Objecttype
     , CASE t.kenmerk
           WHEN 'maakt deel uit van' THEN 'ligt in BAG:Pand'
           WHEN 'heeft als hoofdadres' THEN 'heeft hoofdadres'
           WHEN 'heeft als nevenadres' THEN 'heeft nevenadres'
           WHEN 'ligt aan' THEN 'ligt aan BAG:Openbare ruimte'
           WHEN 'ligt in' THEN 'ligt in BAG:Woonplaats'
           ELSE t.kenmerk
    END                                                                 AS kenmerk
     , t.inonderzoek
     , t.documentnummer
     , t.documentdatum
     , t.begin_geldigheid                                               AS beginGeldigheid
     , t.eind_geldigheid                                                AS eindGeldigheid
     , nvl(t.eind_geldigheid,
           case t.inonderzoek
               when 'N' then t.begin_geldigheid
               end)                                                     AS expirationdate
     , to_date(t.tijdstip_registratie, 'YYYY-MM-DD"T"HH24:MI:SS".000"') AS tijdstipRegistratie
     , to_date(t.eind_registratie, 'YYYY-MM-DD"T"HH24:MI:SS".000"')     AS EindRegistratie
FROM G0363_LVBAG.INONDERZOEK t
