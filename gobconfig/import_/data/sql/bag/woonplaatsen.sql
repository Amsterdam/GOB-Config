WITH
    -- Alle onderzoeken voor deze objectklasse
    in_onderzoeken AS (SELECT *
                         FROM lvbag.inonderzoek
                        WHERE objecttype = 113),
    -- All onderzoeken gegroepeerd per dag op maximum versie
    -- Onderzoeken die meerdere statussen hebben per dag worden beoordeeld op de status aan het einde van de dag
    in_onderzoeken_eod AS (SELECT identificatie
                                , begin_geldigheid
                                , max(versie_identificatie) AS maxversie
                           FROM in_onderzoeken
                           GROUP BY identificatie, begin_geldigheid)
SELECT w.woonplaatsnummer                                                                     AS identificatie
     , w.woonplaatsvolgnummer                                                                 AS volgnummer
     , s.status                                                                               AS status_code
     , s.omschrijving                                                                         AS status_omschrijving
     , to_char(w.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')                                        AS begin_geldigheid
     , to_char(q2.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')                                       AS eind_geldigheid
     , (SELECT CASE
               WHEN listagg(inonderzoek, ';') WITHIN GROUP (ORDER BY object_identificatie) LIKE '%J%'
               THEN 'J'
               ELSE 'N'
               END
        FROM lvbag.inonderzoek io
             INNER JOIN in_onderzoeken_eod io_eod
                     ON io.identificatie = io_eod.identificatie AND io.versie_identificatie = io_eod.maxversie
        WHERE object_identificatie = w.woonplaatsnummer
          AND inonderzoek = 'J'
          AND (
                (
                    -- eindgeldigheid van object is altijd later dan begingeldigheid van onderzoek
                    nvl(to_char(q2.datumopvoer, 'YYYY-MM-DD'), '2199-12-31') > io.begin_geldigheid AND
                    -- begingeldigheid van object is altijd eerder dan eindgeldigheid van onderzoek
                    to_char(w.datumopvoer, 'YYYY-MM-DD') < nvl(io.eind_geldigheid, '2199-12-31') AND
                   -- Er dient gekeken te worden naar de gerelateerde objecten bij een eindgeldigheid van een cyclus.
                    nvl(to_char(q2.datumopvoer, 'YYYY-MM-DD'), '2199-12-31') <= nvl(io.eind_geldigheid, '2199-12-31')
                )
                OR
                (
                    -- begingeldigheid van object is gelijk aan begingeldigheid van onderzoek
                    to_char(w.datumopvoer, 'YYYY-MM-DD') = io.begin_geldigheid AND
                    -- neem laatste cyclus van object, indien begingeldigheid en geldigheid van object gelijk zijn
                    to_char(w.datumopvoer, 'YYYY-MM-DD') = to_char(q2.datumopvoer, 'YYYY-MM-DD') AND
                    -- Er dient gekeken te worden naar de gerelateerde objecten bij een eindgeldigheid van een cyclus.
                    nvl(to_char(q2.datumopvoer, 'YYYY-MM-DD'), '2199-12-31') <= nvl(io.eind_geldigheid, '2199-12-31')
                )
            ))                                                                                AS aanduiding_in_onderzoek
     , (SELECT listagg(identificatie, ';')
        FROM in_onderzoeken io
        WHERE object_identificatie = w.woonplaatsnummer
          AND (
                (
                    -- eindgeldigheid van object is altijd later dan begingeldigheid van onderzoek
                    nvl(to_char(q2.datumopvoer, 'YYYY-MM-DD'), '2199-12-31') > io.begin_geldigheid AND
                    -- begingeldigheid van object is altijd eerder dan eindgeldigheid van onderzoek
                    to_char(w.datumopvoer, 'YYYY-MM-DD') < nvl(io.eind_geldigheid, '2199-12-31') AND
                   -- Er dient gekeken te worden naar de gerelateerde objecten bij een eindgeldigheid van een cyclus.
                    nvl(to_char(q2.datumopvoer, 'YYYY-MM-DD'), '2199-12-31') <= nvl(io.eind_geldigheid, '2199-12-31')
                )
                OR
                (
                    -- begingeldigheid van object is gelijk aan begingeldigheid van onderzoek
                    to_char(w.datumopvoer, 'YYYY-MM-DD') = io.begin_geldigheid AND
                    -- neem laatste cyclus van object, indien begingeldigheid en geldigheid van object gelijk zijn
                    to_char(w.datumopvoer, 'YYYY-MM-DD') = to_char(q2.datumopvoer, 'YYYY-MM-DD') AND
                    -- Er dient gekeken te worden naar de gerelateerde objecten bij een eindgeldigheid van een cyclus.
                    nvl(to_char(q2.datumopvoer, 'YYYY-MM-DD'), '2199-12-31') <= nvl(io.eind_geldigheid, '2199-12-31')
                )
            ))                                                                                AS heeft_onderzoeken
     , w.indgeconstateerd                                                                     AS geconstateerd
     , to_char(w.dd_document, 'YYYY-MM-DD')                                                   AS documentdatum
     , w.documentnummer                                                                       AS documentnummer
     , TRIM(NVL(regexp_substr(w.documentnummer, '^(.*?)_', 1, 1, NULL, 1), w.documentnummer)) AS dossier
     , w.naam                                                                                 AS naam
     , w.woonplaats_ptt                                                                       AS naam_ptt
     , '0363'                                                                                 AS ligt_in_gemeente
     , to_char(w.creation, 'YYYY-MM-DD HH24:MI:SS')                                           AS registratiedatum
     , w.woonplaats_id                                                                        AS source_id
     , NVL2(q2.datumopvoer,
            CASE
            WHEN q2.datumopvoer < sysdate
            THEN to_char(q2.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')
            ELSE to_char(w.modification, 'YYYY-MM-DD HH24:MI:SS')
            END
    , CASE
      WHEN s.status = 2
      THEN CASE
           WHEN q2.datumopvoer < sysdate
           THEN to_char(w.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')
           ELSE to_char(w.creation, 'YYYY-MM-DD HH24:MI:SS')
           END
      ELSE NULL
      END)                                                                                AS expirationdate
     , w.bagproces                                                                            AS bagproces_code
     , m.omschrijving                                                                         AS bagproces_omschrijving
     , sdo_util.to_wktgeometry(geometrie)                                                     AS geometrie
FROM basis.woonplaats w
     -- begindatum gebruiken als einddatum volgende cyclus
         JOIN (SELECT x.woonplaatsnummer
                    , x.woonplaatsvolgnummer
                    , dense_rank() OVER (partition BY x.woonplaatsnummer ORDER BY x.woonplaatsvolgnummer) + 1 AS rang
               FROM basis.woonplaats x
               WHERE x.indauthentiek = 'J') q1 ON w.woonplaatsnummer = q1.woonplaatsnummer AND
                                                  w.woonplaatsvolgnummer = q1.woonplaatsvolgnummer
         LEFT OUTER JOIN (SELECT y.woonplaatsnummer
                               , y.woonplaatsvolgnummer
                               , y.datumopvoer
                               , dense_rank()
                                 OVER (partition BY y.woonplaatsnummer ORDER BY y.woonplaatsvolgnummer) AS rang
                          FROM basis.woonplaats y
                          WHERE y.indauthentiek = 'J') q2 ON q1.woonplaatsnummer = q2.woonplaatsnummer AND
                                                             q1.rang = q2.rang
    -- selecteren status
         LEFT OUTER JOIN basis.woonplaatsstatus s
                         ON w.status_id = s.status
    -- selecteren bagproces / mutatiereden
         LEFT OUTER JOIN basis.mutatiereden m
                         ON w.bagproces = m.id
WHERE  w.indauthentiek = 'J'
