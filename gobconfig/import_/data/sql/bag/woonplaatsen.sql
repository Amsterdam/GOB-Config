WITH
    -- SubQuery factoring for objectklasse dataset
    authentieke_objecten AS (SELECT *
                             FROM   basis.woonplaats
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
     , w.indinonderzoek                                                                       AS aanduiding_in_onderzoek
     , NULL                                                                                   AS heeft_onderzoeken
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
FROM authentieke_objecten w
     -- begindatum gebruiken als einddatum volgende cyclus
	    JOIN begin_cyclus q1 ON w.woonplaatsnummer = q1.woonplaatsnummer AND
	                            w.woonplaatsvolgnummer = q1.woonplaatsvolgnummer
	    LEFT OUTER JOIN eind_cyclus q2 ON  q1.woonplaatsnummer = q2.woonplaatsnummer AND
	                                       q1.rang = q2.rang
    -- selecteren status
         LEFT OUTER JOIN basis.woonplaatsstatus s
                         ON w.status_id = s.status
    -- selecteren bagproces / mutatiereden
         LEFT OUTER JOIN basis.mutatiereden m
                         ON w.bagproces = m.id
