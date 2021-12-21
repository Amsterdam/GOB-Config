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
                 GROUP BY adres_id, adresnummer)
SELECT s.standplaatsnummer                                                                    AS identificatie
     , s.standplaatsvolgnummer                                                                AS volgnummer
     , q4.ligt_in_woonplaats																  AS ligt_in_woonplaats
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
    -- selecteren woonplaatsen via: openbareruimtes en nummeraanduidingen
          LEFT OUTER JOIN (
            WITH
            adressen AS (SELECT   adres_id, adresnummer, OPENBARERUIMTE_ID
                             FROM     G0363_Basis.adres
                             WHERE    indauthentiek = 'J'
                             GROUP BY adres_id, adresnummer, OPENBARERUIMTE_ID),
            standplaatsen_or AS (
                 SELECT
                    sa.STANDPLAATS_ID, sa.STANDPLAATSVOLGNUMMER, a.OPENBARERUIMTE_ID
                FROM
                    G0363_Basis.standplaats_adres sa
                JOIN adressen a
                    ON a.adres_id = sa.adres_id
                GROUP BY sa.STANDPLAATS_ID, sa.STANDPLAATSVOLGNUMMER, a.OPENBARERUIMTE_ID
            )
            SELECT
                   sor.STANDPLAATS_ID,
                   sor.STANDPLAATSVOLGNUMMER,
                   listagg(DISTINCT q2.ligt_in_woonplaats, ';')
                       WITHIN GROUP (ORDER BY q2.ligt_in_woonplaats) AS ligt_in_woonplaats
            FROM standplaatsen_or sor
            -- selecteren openbare ruimte
            LEFT JOIN (
                SELECT DISTINCT o.openbareruimte_id, o.openbareruimtenummer
                FROM G0363_Basis.openbareruimte o
                WHERE o.indauthentiek = 'J'
            ) q1
                ON sor.OPENBARERUIMTE_ID = q1.openbareruimte_id
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
            GROUP BY sor.STANDPLAATS_ID, sor.STANDPLAATSVOLGNUMMER
         ) q4
              ON s.standplaats_id = q4.standplaats_id AND s.standplaatsvolgnummer = q4.standplaatsvolgnummer
-- filter Weesp (3631 or 1012)
-- https://dev.azure.com/CloudCompetenceCenter/Datateam%20Basis%20en%20Kernregistraties/_workitems/edit/25491
WHERE q4.ligt_in_woonplaats NOT IN ('1012;3631', '1012', '3631')
