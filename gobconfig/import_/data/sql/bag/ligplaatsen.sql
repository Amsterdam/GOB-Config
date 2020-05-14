WITH
    -- Alle onderzoeken voor deze objectklasse
    in_onderzoeken AS (SELECT *
                       FROM lvbag.inonderzoek
                       WHERE objecttype = 111),
    -- All onderzoeken gegroepeerd per dag op maximum versie
    -- Onderzoeken die meerdere statussen hebben per dag worden beoordeeld op de status aan het einde van de dag
    in_onderzoeken_eod AS (SELECT identificatie
                                , begin_geldigheid
                                , max(versie_identificatie) AS maxversie
                           FROM in_onderzoeken
                           GROUP BY identificatie, begin_geldigheid)
SELECT l.ligplaatsnummer                                                                      AS identificatie
     , l.ligplaatsvolgnummer                                                                  AS volgnummer
     , l.indgeconstateerd                                                                     AS geconstateerd
     , to_char(l.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')                                        AS begin_geldigheid
     , to_char(q2.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')                                       AS eind_geldigheid
     , (SELECT CASE
               WHEN listagg(inonderzoek, ';') WITHIN GROUP (ORDER BY object_identificatie) LIKE '%J%'
               THEN 'J'
               ELSE 'N'
               END
        FROM lvbag.inonderzoek io
             INNER JOIN in_onderzoeken_eod io_eod
                     ON io.identificatie = io_eod.identificatie AND io.versie_identificatie = io_eod.maxversie
        WHERE object_identificatie = l.ligplaatsnummer
          AND inonderzoek = 'J'
          AND (
                (
                    -- eindgeldigheid van object is altijd later dan begingeldigheid van onderzoek
                    nvl(to_char(q2.datumopvoer, 'YYYY-MM-DD'), '2199-12-31') > io.begin_geldigheid AND
                    -- begingeldigheid van object is altijd eerder dan eindgeldigheid van onderzoek
                    to_char(l.datumopvoer, 'YYYY-MM-DD') < nvl(io.eind_geldigheid, '2199-12-31') AND
                   -- Er dient gekeken te worden naar de gerelateerde objecten bij een eindgeldigheid van een cyclus.
                    nvl(to_char(q2.datumopvoer, 'YYYY-MM-DD'), '2199-12-31') <= nvl(io.eind_geldigheid, '2199-12-31')
                )
                OR
                (
                    -- begingeldigheid van object is gelijk aan begingeldigheid van onderzoek
                    to_char(l.datumopvoer, 'YYYY-MM-DD') = io.begin_geldigheid AND
                    -- neem laatste cyclus van object, indien begingeldigheid en geldigheid van object gelijk zijn
                    to_char(l.datumopvoer, 'YYYY-MM-DD') = to_char(q2.datumopvoer, 'YYYY-MM-DD') AND
                    -- Er dient gekeken te worden naar de gerelateerde objecten bij een eindgeldigheid van een cyclus.
                    nvl(to_char(q2.datumopvoer, 'YYYY-MM-DD'), '2199-12-31') <= nvl(io.eind_geldigheid, '2199-12-31')
                )
            ))                                                                                AS aanduiding_in_onderzoek
     , (SELECT listagg(identificatie, ';')
        FROM in_onderzoeken io
        WHERE object_identificatie = l.ligplaatsnummer
          AND (
                (
                    -- eindgeldigheid van object is altijd later dan begingeldigheid van onderzoek
                    nvl(to_char(q2.datumopvoer, 'YYYY-MM-DD'), '2199-12-31') > io.begin_geldigheid AND
                    -- begingeldigheid van object is altijd eerder dan eindgeldigheid van onderzoek
                    to_char(l.datumopvoer, 'YYYY-MM-DD') < nvl(io.eind_geldigheid, '2199-12-31') AND
                   -- Er dient gekeken te worden naar de gerelateerde objecten bij een eindgeldigheid van een cyclus.
                    nvl(to_char(q2.datumopvoer, 'YYYY-MM-DD'), '2199-12-31') <= nvl(io.eind_geldigheid, '2199-12-31')
                )
                OR
                (
                    -- begingeldigheid van object is gelijk aan begingeldigheid van onderzoek
                    to_char(l.datumopvoer, 'YYYY-MM-DD') = io.begin_geldigheid AND
                    -- neem laatste cyclus van object, indien begingeldigheid en geldigheid van object gelijk zijn
                    to_char(l.datumopvoer, 'YYYY-MM-DD') = to_char(q2.datumopvoer, 'YYYY-MM-DD') AND
                    -- Er dient gekeken te worden naar de gerelateerde objecten bij een eindgeldigheid van een cyclus.
                    nvl(to_char(q2.datumopvoer, 'YYYY-MM-DD'), '2199-12-31') <= nvl(io.eind_geldigheid, '2199-12-31')
                )
            ))                                                                                AS heeft_onderzoeken
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
     , NVL2(q2.datumopvoer,
            CASE
            WHEN q2.datumopvoer < sysdate
            THEN to_char(q2.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')
            ELSE to_char(l.modification, 'YYYY-MM-DD HH24:MI:SS')
            END
    , CASE
      WHEN s.status = 2
      THEN CASE
           WHEN q2.datumopvoer < sysdate
           THEN to_char(l.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')
           ELSE to_char(l.creation, 'YYYY-MM-DD HH24:MI:SS')
           END
      ELSE NULL
      END)                                                                                    AS expirationdate
FROM basis.ligplaats l
     -- begindatum gebruiken als einddatum volgende cyclus
         JOIN (SELECT x.ligplaatsnummer
                    , x.ligplaatsvolgnummer
                    , dense_rank() OVER (partition BY x.ligplaatsnummer ORDER BY x.ligplaatsvolgnummer) + 1 AS rang
               FROM basis.ligplaats x
               WHERE x.indauthentiek = 'J') q1 ON l.ligplaatsnummer = q1.ligplaatsnummer AND
                                                  l.ligplaatsvolgnummer = q1.ligplaatsvolgnummer
         LEFT OUTER JOIN (SELECT y.ligplaatsnummer
                               , y.ligplaatsvolgnummer
                               , y.datumopvoer
                               , dense_rank()
                                 OVER (partition BY y.ligplaatsnummer ORDER BY y.ligplaatsvolgnummer) AS rang
                          FROM basis.ligplaats y
                          WHERE y.indauthentiek = 'J') q2 ON q1.ligplaatsnummer = q2.ligplaatsnummer AND
                                                             q1.rang = q2.rang
    -- selecteren status
         LEFT OUTER JOIN basis.ligplaatsstatus s ON l.status = s.status
    -- selecteren bagproces / mutatiereden
         LEFT OUTER JOIN basis.mutatiereden m ON l.bagproces = m.id
    -- selecteren hoofdadres(sen)
         LEFT OUTER JOIN (SELECT la.ligplaats_id
                               , la.ligplaatsvolgnummer
                               , a.adresnummer
                          FROM basis.ligplaats_adres la
                                   JOIN (SELECT adres_id, adresnummer
                                         FROM basis.adres
                                         WHERE indauthentiek = 'J'
                                         GROUP BY adres_id, adresnummer) a ON a.adres_id = la.adres_id
                          WHERE la.indhoofdadres = 'J') q1 ON l.ligplaats_id = q1.ligplaats_id AND
                                                              l.ligplaatsvolgnummer = q1.ligplaatsvolgnummer
    -- selecteren nevenadres(sen)
         LEFT OUTER JOIN (SELECT la.ligplaats_id
                               , la.ligplaatsvolgnummer
                               , listagg(a.adresnummer, ';')
                                 WITHIN GROUP (ORDER BY la.ligplaats_id,la.ligplaatsvolgnummer) AS adresnummer
                          FROM basis.ligplaats_adres la
                                   JOIN (SELECT adres_id, adresnummer
                                         FROM basis.adres
                                         WHERE indauthentiek = 'J'
                                         GROUP BY adres_id, adresnummer) a ON a.adres_id = la.adres_id
                          WHERE la.indhoofdadres = 'N'
                          GROUP BY la.ligplaats_id, la.ligplaatsvolgnummer) q2 ON l.ligplaats_id = q2.ligplaats_id AND
                                                                                  l.ligplaatsvolgnummer = q2.ligplaatsvolgnummer
WHERE l.indauthentiek = 'J'
