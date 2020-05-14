WITH
    -- Alle onderzoeken voor deze objectklasse
    in_onderzoeken AS (SELECT *
                       FROM lvbag.inonderzoek
                       WHERE objecttype = 20),
    -- All onderzoeken gegroepeerd per dag op maximum versie
    -- Onderzoeken die meerdere statussen hebben per dag worden beoordeeld op de status aan het einde van de dag
    in_onderzoeken_eod AS (SELECT identificatie
                                , begin_geldigheid
                                , max(versie_identificatie) AS maxversie
                           FROM in_onderzoeken
                           GROUP BY identificatie, begin_geldigheid)
SELECT o.openbareruimtenummer                                                                 AS identificatie
     , o.openbareruimtevolgnummer                                                             AS volgnummer
     , o.status_id                                                                            AS status_code
     , s.omschrijving                                                                         AS status_omschrijving
     , to_char(o.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')                                        AS begin_geldigheid
     , to_char(q2.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')                                       AS eind_geldigheid
     , o.indgeconstateerd                                                                     AS geconstateerd
     , (SELECT CASE
               WHEN listagg(inonderzoek, ';') WITHIN GROUP (ORDER BY object_identificatie) LIKE '%J%'
               THEN 'J'
               ELSE 'N'
               END
        FROM lvbag.inonderzoek io
             INNER JOIN in_onderzoeken_eod io_eod
                     ON io.identificatie = io_eod.identificatie AND io.versie_identificatie = io_eod.maxversie
        WHERE object_identificatie = o.openbareruimtenummer
          AND inonderzoek = 'J'
          AND (
                (
                    -- eindgeldigheid van object is altijd later dan begingeldigheid van onderzoek
                    nvl(to_char(q2.datumopvoer, 'YYYY-MM-DD'), '2199-12-31') > io.begin_geldigheid AND
                    -- begingeldigheid van object is altijd eerder dan eindgeldigheid van onderzoek
                    to_char(o.datumopvoer, 'YYYY-MM-DD') < nvl(io.eind_geldigheid, '2199-12-31') AND
                   -- Er dient gekeken te worden naar de gerelateerde objecten bij een eindgeldigheid van een cyclus.
                    nvl(to_char(q2.datumopvoer, 'YYYY-MM-DD'), '2199-12-31') <= nvl(io.eind_geldigheid, '2199-12-31')
                )
                OR
                (
                    -- begingeldigheid van object is gelijk aan begingeldigheid van onderzoek
                    to_char(o.datumopvoer, 'YYYY-MM-DD') = io.begin_geldigheid AND
                    -- neem laatste cyclus van object, indien begingeldigheid en geldigheid van object gelijk zijn
                    to_char(o.datumopvoer, 'YYYY-MM-DD') = to_char(q2.datumopvoer, 'YYYY-MM-DD') AND
                    -- Er dient gekeken te worden naar de gerelateerde objecten bij een eindgeldigheid van een cyclus.
                    nvl(to_char(q2.datumopvoer, 'YYYY-MM-DD'), '2199-12-31') <= nvl(io.eind_geldigheid, '2199-12-31')
                )
            ))                                                                                AS aanduiding_in_onderzoek
     , (SELECT listagg(identificatie, ';')
        FROM in_onderzoeken io
        WHERE object_identificatie = o.openbareruimtenummer
          AND (
                (
                    -- eindgeldigheid van object is altijd later dan begingeldigheid van onderzoek
                    nvl(to_char(q2.datumopvoer, 'YYYY-MM-DD'), '2199-12-31') > io.begin_geldigheid AND
                    -- begingeldigheid van object is altijd eerder dan eindgeldigheid van onderzoek
                    to_char(o.datumopvoer, 'YYYY-MM-DD') < nvl(io.eind_geldigheid, '2199-12-31') AND
                   -- Er dient gekeken te worden naar de gerelateerde objecten bij een eindgeldigheid van een cyclus.
                    nvl(to_char(q2.datumopvoer, 'YYYY-MM-DD'), '2199-12-31') <= nvl(io.eind_geldigheid, '2199-12-31')
                )
                OR
                (
                    -- begingeldigheid van object is gelijk aan begingeldigheid van onderzoek
                    to_char(o.datumopvoer, 'YYYY-MM-DD') = io.begin_geldigheid AND
                    -- neem laatste cyclus van object, indien begingeldigheid en geldigheid van object gelijk zijn
                    to_char(o.datumopvoer, 'YYYY-MM-DD') = to_char(q2.datumopvoer, 'YYYY-MM-DD') AND
                    -- Er dient gekeken te worden naar de gerelateerde objecten bij een eindgeldigheid van een cyclus.
                    nvl(to_char(q2.datumopvoer, 'YYYY-MM-DD'), '2199-12-31') <= nvl(io.eind_geldigheid, '2199-12-31')
                )
            ))                                                                                AS heeft_onderzoeken
     , to_char(o.dd_document, 'YYYY-MM-DD')                                                   AS documentdatum
     , o.documentnummer                                                                       AS documentnummer
     , TRIM(nvl(regexp_substr(o.documentnummer, '^(.*?)_', 1, 1, NULL, 1), o.documentnummer)) AS dossier
     , o.naam                                                                                 AS naam
     , o.straat_nen                                                                           AS naam_nen
     , o.straat_ptt                                                                           AS naam_ptt
     , o.straatcode                                                                           AS straatcode
     , q.woonplaatsnummer                                                                     AS ligt_in_bag_woonplaats
     , o.openbareruimtetype                                                                   AS type_code
     , t.omschrijving                                                                         AS type_omschrijving
     , CASE o.tekst
       WHEN '16'
       THEN NULL
       ELSE o.tekst
       END                                                                                    AS beschrijving_naam
     , o.bagproces                                                                            AS bagproces_code
     , m.omschrijving                                                                         AS bagproces_omschrijving
     , to_char(o.creation, 'YYYY-MM-DD HH24:MI:SS')                                           AS registratiedatum
     , o.openbareruimte_id                                                                    AS source_id
     , NVL2(q2.datumopvoer,
            CASE
            WHEN q2.datumopvoer < sysdate
            THEN to_char(q2.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')
            ELSE to_char(o.modification, 'YYYY-MM-DD HH24:MI:SS')
            END
    , CASE
      WHEN s.status = 2
      THEN CASE
           WHEN q2.datumopvoer < sysdate
           THEN to_char(o.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')
           ELSE to_char(o.creation, 'YYYY-MM-DD HH24:MI:SS')
           END
      ELSE NULL
      END)                                                                                    AS expirationdate
     , sdo_util.to_wktgeometry(o.geometrie)                                                   AS geometrie
FROM basis.openbareruimte o
    -- begindatum gebruiken als einddatum volgende cyclus
         JOIN (SELECT x.openbareruimtenummer
                    , x.openbareruimtevolgnummer
                    , dense_rank() OVER (partition BY x.openbareruimtenummer ORDER BY x.openbareruimtevolgnummer) +
                      1 AS rang
               FROM basis.openbareruimte x
               WHERE x.indauthentiek = 'J') q1 ON o.openbareruimtenummer = q1.openbareruimtenummer AND
                                                  o.openbareruimtevolgnummer = q1.openbareruimtevolgnummer
         LEFT OUTER JOIN (SELECT y.openbareruimtenummer
                               , y.openbareruimtevolgnummer
                               , y.datumopvoer
                               , dense_rank()
                                 OVER (partition BY y.openbareruimtenummer ORDER BY y.openbareruimtevolgnummer) AS rang
                          FROM basis.openbareruimte y
                          WHERE y.indauthentiek = 'J') q2 ON q1.openbareruimtenummer = q2.openbareruimtenummer AND
                                                             q1.rang = q2.rang
    -- selecteren woonplaats
         LEFT OUTER JOIN (SELECT w.woonplaats_id
                               , w.woonplaatsnummer
                          FROM basis.woonplaats w
                          WHERE w.indauthentiek = 'J'
                          GROUP BY w.woonplaats_id
                                 , w.woonplaatsnummer) q ON o.woonplaats_id = q.woonplaats_id
    -- selecteren openbare ruimte type
         LEFT OUTER JOIN basis.openbareruimtetype t ON o.openbareruimtetype = t.code
    -- selecteren status
         LEFT OUTER JOIN basis.openbareruimtestatus s ON o.status_id = s.status
    -- selecteren bagproces / mutatiereden
         LEFT OUTER JOIN basis.mutatiereden m ON o.bagproces = m.id
WHERE o.indauthentiek = 'J'
