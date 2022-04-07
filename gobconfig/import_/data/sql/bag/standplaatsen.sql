WITH
    -- SubQuery factoring for objectklasse dataset
    authentieke_objecten AS (SELECT *
                             FROM   G0363_Basis.standplaats
                             WHERE  indauthentiek = 'J'),
    -- SubQuery factoring for begin and eindgeldigheid
    -- begindatum gebruiken als einddatum volgende cyclus
    begin_cyclus AS (SELECT standplaatsnummer
	                      , standplaatsvolgnummer
                          , datumopvoer
	                      , 1 + dense_rank() OVER (partition BY standplaatsnummer ORDER BY standplaatsvolgnummer) AS rang
	                 FROM   authentieke_objecten),
    eind_cyclus AS (SELECT standplaatsnummer
	                     , standplaatsvolgnummer
	                     , datumopvoer
	                     , dense_rank() OVER (partition BY standplaatsnummer ORDER BY standplaatsvolgnummer) AS rang
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
SELECT s.standplaatsnummer                                                                    AS identificatie
     , s.standplaatsvolgnummer                                                                AS volgnummer
     , s.indgeconstateerd                                                                     AS geconstateerd
     , to_char(s.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')                                        AS begin_geldigheid
     , to_char(q2.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')                                       AS eind_geldigheid
     , NULL                                                                                   AS heeft_onderzoeken
     , t.status                                                                               AS status_code
     , t.omschrijving                                                                         AS status_omschrijving
     , q1.adresnummer                                                                         AS nummeraanduidingid_hoofd
     , q2.adresnummer                                                                         AS nummeraanduidingid_neven
     , sdo_util.to_wktgeometry(s.geometrie)                                                   AS geometrie
     , to_char(s.dd_document, 'YYYY-MM-DD')                                                   AS documentdatum
     , s.documentnummer                                                                       AS documentnummer
     , TRIM(nvl(regexp_substr(s.documentnummer, '^(.*?)_', 1, 1, NULL, 1), s.documentnummer)) AS dossier
     , s.bagproces                                                                            AS bagproces_code
     , m.omschrijving                                                                         AS bagproces_omschrijving
     , to_char(s.creation, 'YYYY-MM-DD HH24:MI:SS')                                           AS registratiedatum
     , s.standplaats_id                                                                       AS source_id
     , s.vrijetekst2                                                                          AS gebruiksdoel
     , NVL2(q2.datumopvoer,
            CASE
            WHEN q2.datumopvoer < sysdate
            THEN to_char(q2.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')
            ELSE to_char(s.modification, 'YYYY-MM-DD HH24:MI:SS')
            END
    , CASE
      WHEN s.status = 2 THEN
          CASE
          WHEN q2.datumopvoer < sysdate
          THEN to_char(s.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')
          ELSE to_char(s.creation, 'YYYY-MM-DD HH24:MI:SS')
          END
      ELSE NULL
      END)                                                                                    AS expirationdate
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
FROM authentieke_objecten s
     -- begindatum gebruiken als einddatum volgende cyclus
	    JOIN begin_cyclus q1 ON s.standplaatsnummer = q1.standplaatsnummer AND
	                            s.standplaatsvolgnummer = q1.standplaatsvolgnummer
	    LEFT OUTER JOIN eind_cyclus q2 ON  q1.standplaatsnummer = q2.standplaatsnummer AND
	                                       q1.rang = q2.rang
    -- selecteren status
         LEFT OUTER JOIN G0363_Basis.standplaatsstatus t ON s.status = t.status
    -- selecteren bagproces / mutatiereden
         LEFT OUTER JOIN G0363_Basis.mutatiereden m ON s.bagproces = m.id
    -- selecteren hoofdadres(sen)
         LEFT OUTER JOIN (SELECT sa.standplaats_id
                               , sa.standplaatsvolgnummer
                               , a.adresnummer
                          FROM G0363_Basis.standplaats_adres sa
                                   JOIN adressen a ON a.adres_id = sa.adres_id
                          WHERE sa.indhoofdadres = 'J') q1 ON s.standplaats_id = q1.standplaats_id AND
                                                              s.standplaatsvolgnummer = q1.standplaatsvolgnummer
    -- selecteren nevenadres(sen)
         LEFT OUTER JOIN (SELECT sa.standplaats_id
                               , sa.standplaatsvolgnummer
                               , listagg(a.adresnummer, ';')
                                 WITHIN GROUP (ORDER BY sa.standplaats_id,sa.standplaatsvolgnummer) AS adresnummer
                          FROM G0363_Basis.standplaats_adres sa
                                   JOIN adressen a ON a.adres_id = sa.adres_id
                          WHERE sa.indhoofdadres = 'N'
                          GROUP BY sa.standplaats_id, sa.standplaatsvolgnummer) q2 ON s.standplaats_id = q2.standplaats_id AND
                                                                                      s.standplaatsvolgnummer = q2.standplaatsvolgnummer
        -- selecteren ligt_in_gemeente
         LEFT OUTER JOIN adres_in_gemeente q3 ON q3.ADRESNUMMER = q1.ADRESNUMMER
