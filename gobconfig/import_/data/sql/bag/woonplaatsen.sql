WITH
    -- SubQuery factoring for objectklasse dataset
    authentieke_objecten AS (SELECT *
                             FROM   G0363_Basis.woonplaats
                             WHERE  indauthentiek = 'J'),
    -- SubQuery factoring for begin and eindgeldigheid
    -- begindatum gebruiken als einddatum volgende cyclus
    begin_cyclus AS (SELECT woonplaatsnummer
	                      , woonplaatsvolgnummer
                          , datumopvoer
	                      , 1 + dense_rank() OVER (partition BY woonplaatsnummer ORDER BY woonplaatsvolgnummer) AS rang
	                 FROM   authentieke_objecten),
    eind_cyclus AS (SELECT woonplaatsnummer
	                     , woonplaatsvolgnummer
	                     , datumopvoer
	                     , dense_rank() OVER (partition BY woonplaatsnummer ORDER BY woonplaatsvolgnummer) AS rang
	                FROM   authentieke_objecten)
 SELECT w.woonplaatsnummer                                                                     AS identificatie
      , w.woonplaatsvolgnummer                                                                 AS volgnummer
      , s.status                                                                               AS status_code
      , s.omschrijving                                                                         AS status_omschrijving
      , to_char(w.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')                                        AS begin_geldigheid
      , to_char(q2.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')                                       AS eind_geldigheid
      , NULL                                                                                   AS heeft_onderzoeken
      , w.indgeconstateerd                                                                     AS geconstateerd
      , to_char(w.dd_document, 'YYYY-MM-DD')                                                   AS documentdatum
      , w.documentnummer                                                                       AS documentnummer
      , TRIM(NVL(regexp_substr(w.documentnummer, '^(.*?)_', 1, 1, NULL, 1), w.documentnummer)) AS dossier
      , w.naam                                                                                 AS naam
      , w.woonplaats_ptt                                                                       AS naam_ptt
      , CASE
      -- Gemeente Weesp:
      --   woonplaats Weesp and ligplaats closed before 24 maart 2022
      WHEN (w.woonplaatsnummer IN (1012, 3631) AND q2.DATUMOPVOER < DATE '2022-03-24')
        THEN '0457'
      -- Gemeente Amsterdam:
      --   woonplaats Weesp and ligplaats closed after 24 maart 2022 or actual
      --   woonplaats Amsterdam (no time condition)
      WHEN w.woonplaatsnummer IN (1024, 1025, 3594) OR
           (w.woonplaatsnummer IN (1012, 3631) AND (q2.DATUMOPVOER >= DATE '2022-03-24' OR q2.datumopvoer IS NULL))
        THEN '0363'
      ELSE
        NULL  -- gemeente is undetermined
      END                                                                                      AS ligt_in_gemeente
      , to_char(w.creation, 'YYYY-MM-DD HH24:MI:SS')                                           AS registratiedatum
      , w.woonplaats_id                                                                        AS source_id

     , CASE
         -- no endvalidity, use beginvalidity for certain status
         WHEN q2.datumopvoer IS NULL
         THEN
             CASE
                 -- when status = 2,, the verblijfsobject is expired at begin_geldigheid
                 WHEN s.status = 2
                 THEN to_char(w.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')
             END
          -- endvalidity exists
         ELSE
             CASE
                 WHEN q2.datumopvoer < sysdate
                 THEN to_char(q2.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')
                 ELSE to_char(w.modification, 'YYYY-MM-DD HH24:MI:SS')
             END
       END                                                                                     AS expirationdate
      , w.bagproces                                                                            AS bagproces_code
      , m.omschrijving                                                                         AS bagproces_omschrijving
      , sdo_util.to_wktgeometry(geometrie)                                                     AS geometrie
 FROM authentieke_objecten w
          -- begindatum gebruiken als einddatum volgende cyclus
          JOIN begin_cyclus q1 ON w.woonplaatsnummer = q1.woonplaatsnummer AND
                                  w.woonplaatsvolgnummer = q1.woonplaatsvolgnummer
          LEFT OUTER JOIN eind_cyclus q2 ON q1.woonplaatsnummer = q2.woonplaatsnummer AND
                                            q1.rang = q2.rang
     -- selecteren status
          LEFT OUTER JOIN G0363_Basis.woonplaatsstatus s
                          ON w.status_id = s.status
     -- selecteren bagproces / mutatiereden
          LEFT OUTER JOIN G0363_Basis.mutatiereden m
                          ON w.bagproces = m.id
