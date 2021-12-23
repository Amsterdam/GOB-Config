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
     , y.ligt_in_woonplaats                                                                   AS ligt_in_woonplaats
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
    -- selecteren type woonobject, inclusief ligt_in_woonplaats
        LEFT OUTER JOIN (
              SELECT x1.gebouw_id
                   , x1.gebouwvolgnummer
                   , listagg(DISTINCT x3.ligt_in_woonplaats, ';') as ligt_in_woonplaats
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
                           , vg.verblijfseenheid_id
              ) x1
              JOIN (SELECT vg.verblijfsobject_id
                          , vg.verblijfsobjectvolgnummer
                          , vg.gebruiksdoel_id
                     FROM G0363_Basis.verblijfsobject_gebruiksdoel vg) x2
                ON x1.verblijfseenheid_id = x2.verblijfsobject_id AND x1.verblijfseenheidvolgnummer = x2.verblijfsobjectvolgnummer
              JOIN (
                    -- selecteren woonplaats via verblijfsobject en openbareruimtes
                    WITH
                    adressen AS (SELECT   adres_id, adresnummer, OPENBARERUIMTE_ID
                                     FROM     G0363_Basis.adres
                                     WHERE    indauthentiek = 'J'
                                     GROUP BY adres_id, adresnummer, OPENBARERUIMTE_ID),
                    verblijfseenheid_or AS (
                         SELECT
                            vea.VERBLIJFSEENHEID_ID, vea.VERBLIJFSEENHEIDVOLGNUMMER, a.OPENBARERUIMTE_ID
                        FROM
                            G0363_Basis.verblijfseenheid_adres vea
                        JOIN adressen a
                            ON a.adres_id = vea.adres_id
                        GROUP BY vea.VERBLIJFSEENHEID_ID, vea.VERBLIJFSEENHEIDVOLGNUMMER, a.OPENBARERUIMTE_ID
                    )
                    SELECT
                           vor.VERBLIJFSEENHEID_ID,
                           vor.VERBLIJFSEENHEIDVOLGNUMMER,
                           q2.ligt_in_woonplaats
                    FROM verblijfseenheid_or vor
                    -- selecteren openbare ruimte
                    LEFT JOIN (
                        SELECT DISTINCT o.openbareruimte_id, o.openbareruimtenummer
                        FROM G0363_Basis.openbareruimte o
                        WHERE o.indauthentiek = 'J'
                    ) q1
                        ON vor.OPENBARERUIMTE_ID = q1.openbareruimte_id
                    -- selecteren woonplaats
                    LEFT JOIN (
                           SELECT DISTINCT o.openbareruimte_id, w.woonplaats_id, w.woonplaatsnummer AS ligt_in_woonplaats
                           FROM   G0363_Basis.openbareruimte o
                           JOIN (
                               SELECT woonplaats_id, woonplaatsnummer, indauthentiek
                               FROM G0363_Basis.woonplaats
                           ) w
                               ON o.woonplaats_id = w.woonplaats_id
                           WHERE o.indauthentiek = 'J' AND w.indauthentiek = 'J'
                    ) q2
                        ON q2.openbareruimte_id = q1.openbareruimte_id
              ) x3
                ON x1.verblijfseenheid_id = x3.verblijfseenheid_id AND x1.VERBLIJFSEENHEIDVOLGNUMMER = x3.VERBLIJFSEENHEIDVOLGNUMMER
              GROUP BY x1.gebouw_id, x1.gebouwvolgnummer
        ) y
            ON y.gebouw_id = g.gebouw_id AND y.gebouwvolgnummer = g.gebouwvolgnummer
-- filter Weesp (3631 or 1012)
-- https://dev.azure.com/CloudCompetenceCenter/Datateam%20Basis%20en%20Kernregistraties/_workitems/edit/25491
WHERE y.ligt_in_woonplaats NOT IN ('1012;3631', '1012', '3631')
