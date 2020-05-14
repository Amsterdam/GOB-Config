WITH
    -- Alle onderzoeken voor deze objectklasse
    in_onderzoeken AS (SELECT *
                       FROM lvbag.inonderzoek
                       WHERE objecttype = 112),
    -- All onderzoeken gegroepeerd per dag op maximum versie
    -- Onderzoeken die meerdere statussen hebben per dag worden beoordeeld op de status aan het einde van de dag
    in_onderzoeken_eod AS (SELECT identificatie
                                , begin_geldigheid
                                , max(versie_identificatie) AS maxversie
                           FROM in_onderzoeken
                           GROUP BY identificatie, begin_geldigheid)
SELECT s.standplaatsnummer                                                                    AS identificatie
     , s.standplaatsvolgnummer                                                                AS volgnummer
     , s.indgeconstateerd                                                                     AS geconstateerd
     , to_char(s.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')                                        AS begin_geldigheid
     , to_char(q2.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')                                       AS eind_geldigheid
     , (SELECT CASE
               WHEN listagg(inonderzoek, ';') WITHIN GROUP (ORDER BY object_identificatie) LIKE '%J%'
               THEN 'J'
               ELSE 'N'
               END
        FROM lvbag.inonderzoek io
             INNER JOIN in_onderzoeken_eod io_eod
                     ON io.identificatie = io_eod.identificatie AND io.versie_identificatie = io_eod.maxversie
        WHERE object_identificatie = s.standplaatsnummer
          AND inonderzoek = 'J'
          AND (
                (
                    -- eindgeldigheid van object is altijd later dan begingeldigheid van onderzoek
                    nvl(to_char(q2.datumopvoer, 'YYYY-MM-DD'), '2199-12-31') > io.begin_geldigheid AND
                    -- begingeldigheid van object is altijd eerder dan eindgeldigheid van onderzoek
                    to_char(s.datumopvoer, 'YYYY-MM-DD') < nvl(io.eind_geldigheid, '2199-12-31') AND
                   -- Er dient gekeken te worden naar de gerelateerde objecten bij een eindgeldigheid van een cyclus.
                    nvl(to_char(q2.datumopvoer, 'YYYY-MM-DD'), '2199-12-31') <= nvl(io.eind_geldigheid, '2199-12-31')
                )
                OR
                (
                    -- begingeldigheid van object is gelijk aan begingeldigheid van onderzoek
                    to_char(s.datumopvoer, 'YYYY-MM-DD') = io.begin_geldigheid AND
                    -- neem laatste cyclus van object, indien begingeldigheid en geldigheid van object gelijk zijn
                    to_char(s.datumopvoer, 'YYYY-MM-DD') = to_char(q2.datumopvoer, 'YYYY-MM-DD') AND
                    -- Er dient gekeken te worden naar de gerelateerde objecten bij een eindgeldigheid van een cyclus.
                    nvl(to_char(q2.datumopvoer, 'YYYY-MM-DD'), '2199-12-31') <= nvl(io.eind_geldigheid, '2199-12-31')
                )
            ))                                                                                AS aanduiding_in_onderzoek
     , (SELECT listagg(identificatie, ';')
        FROM in_onderzoeken io
        WHERE object_identificatie = s.standplaatsnummer
          AND (
                (
                    -- eindgeldigheid van object is altijd later dan begingeldigheid van onderzoek
                    nvl(to_char(q2.datumopvoer, 'YYYY-MM-DD'), '2199-12-31') > io.begin_geldigheid AND
                    -- begingeldigheid van object is altijd eerder dan eindgeldigheid van onderzoek
                    to_char(s.datumopvoer, 'YYYY-MM-DD') < nvl(io.eind_geldigheid, '2199-12-31') AND
                   -- Er dient gekeken te worden naar de gerelateerde objecten bij een eindgeldigheid van een cyclus.
                    nvl(to_char(q2.datumopvoer, 'YYYY-MM-DD'), '2199-12-31') <= nvl(io.eind_geldigheid, '2199-12-31')
                )
                OR
                (
                    -- begingeldigheid van object is gelijk aan begingeldigheid van onderzoek
                    to_char(s.datumopvoer, 'YYYY-MM-DD') = io.begin_geldigheid AND
                    -- neem laatste cyclus van object, indien begingeldigheid en geldigheid van object gelijk zijn
                    to_char(s.datumopvoer, 'YYYY-MM-DD') = to_char(q2.datumopvoer, 'YYYY-MM-DD') AND
                    -- Er dient gekeken te worden naar de gerelateerde objecten bij een eindgeldigheid van een cyclus.
                    nvl(to_char(q2.datumopvoer, 'YYYY-MM-DD'), '2199-12-31') <= nvl(io.eind_geldigheid, '2199-12-31')
                )
            ))                                                                                AS heeft_onderzoeken
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
FROM basis.standplaats s
     -- begindatum gebruiken als einddatum volgende cyclus
         JOIN (SELECT x.standplaatsnummer
                    , x.standplaatsvolgnummer
                    , dense_rank() OVER (partition BY x.standplaatsnummer ORDER BY x.standplaatsvolgnummer) + 1 AS rang
               FROM basis.standplaats x
               WHERE x.indauthentiek = 'J') q1 ON s.standplaatsnummer = q1.standplaatsnummer AND
                                                  s.standplaatsvolgnummer = q1.standplaatsvolgnummer
         LEFT OUTER JOIN (SELECT y.standplaatsnummer
                               , y.standplaatsvolgnummer
                               , y.datumopvoer
                               , dense_rank()
                                 OVER (partition BY y.standplaatsnummer ORDER BY y.standplaatsvolgnummer) AS rang
                          FROM basis.standplaats y
                          WHERE y.indauthentiek = 'J') q2 ON q1.standplaatsnummer = q2.standplaatsnummer AND
                                                             q1.rang = q2.rang
    -- selecteren status
         LEFT OUTER JOIN basis.standplaatsstatus t ON s.status = t.status
    -- selecteren bagproces / mutatiereden
         LEFT OUTER JOIN basis.mutatiereden m ON s.bagproces = m.id
    -- selecteren hoofdadres(sen)
         LEFT OUTER JOIN (SELECT sa.standplaats_id
                               , sa.standplaatsvolgnummer
                               , a.adresnummer
                          FROM basis.standplaats_adres sa
                                   JOIN (SELECT adres_id, adresnummer
                                         FROM basis.adres
                                         WHERE indauthentiek = 'J'
                                         GROUP BY adres_id, adresnummer) a ON a.adres_id = sa.adres_id
                          WHERE sa.indhoofdadres = 'J') q1 ON s.standplaats_id = q1.standplaats_id AND
                                                              s.standplaatsvolgnummer = q1.standplaatsvolgnummer
    -- selecteren nevenadres(sen)
         LEFT OUTER JOIN (SELECT sa.standplaats_id
                               , sa.standplaatsvolgnummer
                               , listagg(a.adresnummer, ';')
                                 WITHIN GROUP (ORDER BY sa.standplaats_id,sa.standplaatsvolgnummer) AS adresnummer
                          FROM basis.standplaats_adres sa
                                   JOIN (SELECT adres_id, adresnummer
                                         FROM basis.adres
                                         WHERE indauthentiek = 'J'
                                         GROUP BY adres_id, adresnummer) a ON a.adres_id = sa.adres_id
                          WHERE sa.indhoofdadres = 'N'
                          GROUP BY sa.standplaats_id, sa.standplaatsvolgnummer) q2 ON s.standplaats_id = q2.standplaats_id AND
                                                                                      s.standplaatsvolgnummer = q2.standplaatsvolgnummer
WHERE s.indauthentiek = 'J'
