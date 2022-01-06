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
                 GROUP BY adres_id, adresnummer)
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
                         WHERE la.indhoofdadres = 'J') q1 ON l.ligplaats_id = q1.ligplaats_id AND l.ligplaatsvolgnummer = q1.ligplaatsvolgnummer
    -- selecteren nevenadres(sen)
        LEFT OUTER JOIN (SELECT la.ligplaats_id
                              , la.ligplaatsvolgnummer
                              , listagg(a.adresnummer, ';')
                                WITHIN GROUP (ORDER BY la.ligplaats_id,la.ligplaatsvolgnummer) AS adresnummer
                         FROM G0363_Basis.ligplaats_adres la
                                  JOIN adressen a ON a.adres_id = la.adres_id
                         WHERE la.indhoofdadres = 'N'
                         GROUP BY la.ligplaats_id, la.ligplaatsvolgnummer) q2 ON l.ligplaats_id = q2.ligplaats_id AND l.ligplaatsvolgnummer = q2.ligplaatsvolgnummer
    -- selecteren woonplaatsen via: openbareruimtes en nummeraanduidingen
        LEFT OUTER JOIN (
            SELECT ligplaats_id, ligplaatsvolgnummer, MAX(w.woonplaatsnummer) AS woonplaatsnummer
            FROM G0363_Basis.ligplaats_adres lpa
                JOIN G0363_Basis.adres adr
                     USING (adres_id)
                JOIN G0363_Basis.openbareruimte or_
                    USING (openbareruimte_id)
                JOIN G0363_Basis.woonplaats w
                     ON or_.woonplaats_id = w.woonplaats_id
            WHERE adr.indauthentiek = 'J' AND or_.indauthentiek = 'J' AND w.indauthentiek = 'J'
            GROUP BY ligplaats_id, ligplaatsvolgnummer
        ) w
             ON l.ligplaats_id = w.ligplaats_id AND l.ligplaatsvolgnummer = w.ligplaatsvolgnummer
-- filter Weesp (3631 or 1012)
-- https://dev.azure.com/CloudCompetenceCenter/Datateam%20Basis%20en%20Kernregistraties/_workitems/edit/25491
WHERE w.woonplaatsnummer IN ('1025', '1024', '3594')
   or (w.woonplaatsnummer is null and substr(l.ligplaatsnummer, 0, 4) = '0363')
