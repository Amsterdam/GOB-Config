WITH
    -- SubQuery factoring for objectklasse dataset
    authentieke_objecten AS (SELECT *
                             FROM   G0363_Basis.gebouw
                             WHERE  indauthentiek = 'J'),
    -- SubQuery factoring for begin and eindgeldigheid
    -- begindatum gebruiken als einddatum volgende cyclus
    begin_cyclus AS (SELECT gebouwnummer
	                      , gebouwvolgnummer
                          , datumopvoer
	                      , 1 + dense_rank() OVER (partition BY gebouwnummer ORDER BY gebouwvolgnummer) AS rang
	                 FROM   authentieke_objecten),
    eind_cyclus AS (SELECT gebouwnummer
	                     , gebouwvolgnummer
	                     , datumopvoer
	                     , dense_rank() OVER (partition BY gebouwnummer ORDER BY gebouwvolgnummer) AS rang
	                FROM   authentieke_objecten)
SELECT g.gebouwnummer                                                                         AS identificatie
     , g.gebouwvolgnummer                                                                     AS volgnummer
     , g.indgeconstateerd                                                                     AS geconstateerd
     , to_char(g.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')                                        AS begin_geldigheid
     , to_char(q2.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')                                       AS eind_geldigheid
     , NULL                                                                                   AS heeft_onderzoeken
     , g.status_id                                                                            AS status_code
     , s.omschrijving                                                                         AS status_omschrijving
     , CASE
       WHEN y.aantal_wonen = 1
       THEN 'Eén woning'
       WHEN y.aantal_wonen > 1
       THEN 'Meerdere woningen'
       END                                                                                    AS type_woonobject
     , ROUND(g.aantal_bouwlagen)                                                              AS aantal_bouwlagen
     , ROUND(g.laagste_bouwlaag)                                                              AS laagste_bouwlaag
     , ROUND(g.hoogste_bouwlaag)                                                              AS hoogste_bouwlaag
     , g.gebouwtype                                                                           AS ligging_code
     , t.omschrijving                                                                         AS ligging_omschrijving
     , g.naam                                                                                 AS naam
     , g.bouwjaar                                                                             AS oorspronkelijk_bouwjaar
     , sdo_util.to_wktgeometry(g.geometrie)                                                   AS geometrie
     , to_char(g.dd_document, 'YYYY-MM-DD')                                                   AS documentdatum
     , g.documentnummer                                                                       AS documentnummer
     , TRIM(nvl(regexp_substr(g.documentnummer, '^(.*?)_', 1, 1, NULL, 1), g.documentnummer)) AS dossier
     , g.bagproces                                                                            AS bagproces_code
     , m.omschrijving                                                                         AS bagproces_omschrijving
     , to_char(g.creation, 'YYYY-MM-DD HH24:MI:SS')                                           AS registratiedatum
     , g.gebouw_id                                                                            AS source_id
     , NVL2(q2.datumopvoer,
            CASE
            WHEN q2.datumopvoer < sysdate
            THEN to_char(q2.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')
            ELSE to_char(g.modification, 'YYYY-MM-DD HH24:MI:SS')
            END
    , CASE
      WHEN s.status NOT IN (1, 2, 3, 7, 10, 11, 12, 13)
      THEN CASE
           WHEN q2.datumopvoer < sysdate
           THEN to_char(g.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')
           ELSE to_char(g.creation, 'YYYY-MM-DD HH24:MI:SS')
           END
      ELSE NULL
      END)                                                                                    AS expirationdate
FROM authentieke_objecten g
     -- begindatum gebruiken als einddatum volgende cyclus
	    JOIN begin_cyclus q1 ON g.gebouwnummer = q1.gebouwnummer AND
	                            g.gebouwvolgnummer = q1.gebouwvolgnummer
	    LEFT OUTER JOIN eind_cyclus q2 ON  q1.gebouwnummer = q2.gebouwnummer AND
	                                       q1.rang = q2.rang
    -- selecteren status
         LEFT OUTER JOIN G0363_Basis.gebouwstatus s ON g.status_id = s.status
    -- selecteren bagproces / mutatiereden
         LEFT OUTER JOIN G0363_Basis.mutatiereden m ON g.bagproces = m.id
    -- selecteren ligging
         LEFT OUTER JOIN G0363_Basis.gebouwtype t ON g.gebouwtype = t.gebouwtype
    -- selecteren type woonobject
         LEFT OUTER JOIN (SELECT x1.gebouw_id
                               , x1.gebouwvolgnummer
                               , COUNT(CASE
                                       WHEN x2.gebruiksdoel_id > 1
                                       THEN NULL
                                       ELSE x2.gebruiksdoel_id
                                       END) AS aantal_wonen
                               , COUNT(CASE
                                       WHEN x2.gebruiksdoel_id = 1
                                       THEN NULL
                                       ELSE x2.gebruiksdoel_id
                                       END) AS aantal_nietwonen
                          FROM (SELECT vg.gebouw_id
                                     , vg.gebouwvolgnummer
                                     , vg.verblijfseenheid_id
                                     , MAX(vg.verblijfseenheidvolgnummer) AS verblijfseenheidvolgnummer
                                FROM G0363_Basis.verblijfseenheid_gebouw vg
                                GROUP BY vg.gebouw_id
                                       , vg.gebouwvolgnummer
                                       , vg.verblijfseenheid_id) x1
                                   JOIN (SELECT vg.verblijfsobject_id
                                              , vg.verblijfsobjectvolgnummer
                                              , vg.gebruiksdoel_id
                                         FROM G0363_Basis.verblijfsobject_gebruiksdoel vg) x2 ON x1.verblijfseenheid_id = x2.verblijfsobject_id AND
                                                                                           x1.verblijfseenheidvolgnummer = x2.verblijfsobjectvolgnummer
                          GROUP BY x1.gebouw_id, x1.gebouwvolgnummer
          ) y ON y.gebouw_id = g.gebouw_id AND y.gebouwvolgnummer = g.gebouwvolgnummer
        LEFT JOIN (
            -- selecteer woonplaats via verblijfsobject / nummeraanduiding / openbareruimte / woonplaats
            SELECT gebouw_id, gebouwvolgnummer, MAX(w.woonplaatsnummer) AS woonplaatsnummer
            FROM G0363_Basis.verblijfseenheid_gebouw veg
                     JOIN G0363_Basis.verblijfseenheid_adres vea
                          USING (verblijfseenheid_id, verblijfseenheidvolgnummer)
                     JOIN G0363_Basis.adres adr
                          USING (adres_id)
                     JOIN G0363_Basis.openbareruimte or_
                         USING (openbareruimte_id)
                     JOIN G0363_Basis.woonplaats w
                          ON or_.woonplaats_id = w.woonplaats_id
            WHERE adr.indauthentiek = 'J' AND or_.indauthentiek = 'J' AND w.indauthentiek = 'J'
            GROUP BY gebouw_id, gebouwvolgnummer
        ) w
            ON w.gebouw_id = g.gebouw_id AND w.gebouwvolgnummer = g.gebouwvolgnummer
-- Not every pand has a woonplaats linked to it.
-- Also filter the panden without woonplaats on the first 4 characters of gebouwnummer
WHERE (
  w.woonplaatsnummer IN ('1025', '1024', '3594') OR (w.woonplaatsnummer IS NULL AND SUBSTR(g.gebouwnummer, 0, 4) = '0363')
)
