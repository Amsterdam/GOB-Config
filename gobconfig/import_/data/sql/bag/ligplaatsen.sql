WITH
    -- SubQuery factoring for objectklasse dataset
    authentieke_objecten AS (SELECT *
                             FROM   G0363_Basis.ligplaats
                             WHERE  indauthentiek = 'J'),
    -- SubQuery factoring for begin and eindgeldigheid
    -- begindatum gebruiken als einddatum volgende cyclus
    begin_cyclus AS (SELECT ligplaatsnummer
	                      , ligplaatsvolgnummer
                          , datumopvoer
	                      , 1 + dense_rank() OVER (partition BY ligplaatsnummer ORDER BY ligplaatsvolgnummer) AS rang
	                 FROM   authentieke_objecten),
    eind_cyclus AS (SELECT ligplaatsnummer
	                     , ligplaatsvolgnummer
	                     , datumopvoer
	                     , dense_rank() OVER (partition BY ligplaatsnummer ORDER BY ligplaatsvolgnummer) AS rang
	                FROM   authentieke_objecten),
    -- SubQuery factoring for shared datasets
    adressen AS (SELECT   adres_id
                        , adresnummer
                 FROM     G0363_Basis.adres
                 WHERE    indauthentiek = 'J'
                 GROUP BY adres_id, adresnummer),
     -- gemeentes
    adres_in_gemeente AS (
        SELECT a.ADRESNUMMER, MAX(q3.ligt_in_woonplaats) as ligt_in_woonplaats
        FROM G0363_Basis.adres a
        LEFT OUTER JOIN (
            SELECT w2.openbareruimte_id, MAX(w2.woonplaatsnummer) AS ligt_in_woonplaats
            FROM (
                SELECT o.openbareruimte_id, w.woonplaatsnummer
                  FROM G0363_Basis.openbareruimte o
                  JOIN G0363_Basis.woonplaats w
                    ON o.woonplaats_id = w.woonplaats_id
                  WHERE o.indauthentiek = 'J' AND w.indauthentiek = 'J'
                  GROUP BY o.openbareruimte_id, w.woonplaats_id, w.woonplaatsnummer
                ) w2
            GROUP BY w2.openbareruimte_id
        ) q3
            USING  (openbareruimte_id)
        WHERE a.INDAUTHENTIEK = 'J'
        GROUP BY a.ADRESNUMMER
    )
SELECT l.ligplaatsnummer                                                                      AS identificatie
     , l.ligplaatsvolgnummer                                                                  AS volgnummer
     , l.indgeconstateerd                                                                     AS geconstateerd
     , to_char(l.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')                                        AS begin_geldigheid
     , to_char(q2.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')                                       AS eind_geldigheid
     , NULL                                                                                   AS heeft_onderzoeken
     , l.status                                                                               AS status_code
     , s.omschrijving                                                                         AS status_omschrijving
     , q1.adresnummer                                                                         AS nummeraanduidingid_hoofd
     , q2.adresnummer                                                                         AS nummeraanduidingid_neven
     , sdo_util.to_wktgeometry(l.geometrie)                                                   AS geometrie
     , to_char(l.dd_document, 'YYYY-MM-DD')                                                   AS documentdatum
     , l.documentnummer                                                                       AS documentnummer
     , TRIM(nvl(regexp_substr(l.documentnummer, '^(.*?)_', 1, 1, NULL, 1), l.documentnummer)) AS dossier
     , l.bagproces                                                                            AS bagproces_code
     , m.omschrijving                                                                         AS bagproces_omschrijving
     , to_char(l.creation, 'YYYY-MM-DD HH24:MI:SS')                                           AS registratiedatum
     , l.ligplaats_id                                                                         AS source_id
     , l.vrijetekst2                                                                          AS gebruiksdoel
     , CASE
         WHEN q2.datumopvoer IS NULL
         THEN
             CASE
                 WHEN s.status = 2  -- Plaats ingetrokken
                 THEN to_char(l.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')  -- begin_geldigheid
             END
         ELSE
             CASE
                 WHEN q2.datumopvoer < sysdate
                 THEN to_char(q2.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')  -- eind_geldigheid
                 ELSE to_char(l.modification, 'YYYY-MM-DD HH24:MI:SS')
             END
       END                                                                                    AS expirationdate
    , CASE
      -- Gemeente Weesp:
      --   woonplaats Weesp and ligplaats closed before 24 maart 2022
      WHEN (q3.ligt_in_woonplaats IN (1012, 3631) AND q2.DATUMOPVOER < DATE '2022-03-24')
      THEN '0457'
      -- Gemeente Amsterdam:
      --   woonplaats Weesp and ligplaats closed after 24 maart 2022 or actual
      --   woonplaats Amsterdam (no time condition)
      WHEN q3.ligt_in_woonplaats IN (1024, 1025, 3594) OR
           (q3.ligt_in_woonplaats IN (1012, 3631) AND (q2.DATUMOPVOER >= DATE '2022-03-24' OR q2.datumopvoer IS NULL))
      THEN '0363'
      -- gemeente is undetermined
      ELSE NULL
    END                                                                                       AS ligt_in_gemeente
FROM authentieke_objecten l
     -- begindatum gebruiken als einddatum volgende cyclus
	    JOIN begin_cyclus q1 ON l.ligplaatsnummer = q1.ligplaatsnummer AND
	                            l.ligplaatsvolgnummer = q1.ligplaatsvolgnummer
	    LEFT OUTER JOIN eind_cyclus q2 ON  q1.ligplaatsnummer = q2.ligplaatsnummer AND
	                                       q1.rang = q2.rang
    -- selecteren status
         LEFT OUTER JOIN G0363_Basis.ligplaatsstatus s ON l.status = s.status
    -- selecteren bagproces / mutatiereden
         LEFT OUTER JOIN G0363_Basis.mutatiereden m ON l.bagproces = m.id
    -- selecteren hoofdadres(sen)
         LEFT OUTER JOIN (SELECT la.ligplaats_id
                               , la.ligplaatsvolgnummer
                               , a.adresnummer
                          FROM G0363_Basis.ligplaats_adres la
                                   JOIN adressen a ON a.adres_id = la.adres_id
                          WHERE la.indhoofdadres = 'J') q1 ON l.ligplaats_id = q1.ligplaats_id AND
                                                              l.ligplaatsvolgnummer = q1.ligplaatsvolgnummer
    -- selecteren nevenadres(sen)
         LEFT OUTER JOIN (SELECT la.ligplaats_id
                               , la.ligplaatsvolgnummer
                               , listagg(a.adresnummer, ';')
                                 WITHIN GROUP (ORDER BY la.ligplaats_id,la.ligplaatsvolgnummer) AS adresnummer
                          FROM G0363_Basis.ligplaats_adres la
                                   JOIN adressen a ON a.adres_id = la.adres_id
                          WHERE la.indhoofdadres = 'N'
                          GROUP BY la.ligplaats_id, la.ligplaatsvolgnummer) q2 ON l.ligplaats_id = q2.ligplaats_id AND
                                                                                  l.ligplaatsvolgnummer = q2.ligplaatsvolgnummer
        -- selecteren ligt_in_gemeente
         LEFT OUTER JOIN adres_in_gemeente q3 ON q3.ADRESNUMMER = q1.ADRESNUMMER
