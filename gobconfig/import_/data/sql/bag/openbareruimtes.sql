WITH
    -- SubQuery factoring for objectklasse dataset
    authentieke_objecten AS (SELECT *
                             FROM   basis.openbareruimte
                             WHERE  indauthentiek = 'J'),
    -- SubQuery factoring for begin and eindgeldigheid
    -- begindatum gebruiken als einddatum volgende cyclus
    begin_cyclus AS (SELECT openbareruimtenummer
	                      , openbareruimtevolgnummer
                          , datumopvoer
	                      , 1 + dense_rank() OVER (partition BY openbareruimtenummer ORDER BY openbareruimtevolgnummer) AS rang
	                 FROM   authentieke_objecten),
    eind_cyclus AS (SELECT openbareruimtenummer
	                     , openbareruimtevolgnummer
	                     , datumopvoer
	                     , dense_rank() OVER (partition BY openbareruimtenummer ORDER BY openbareruimtevolgnummer) AS rang
	                FROM   authentieke_objecten)
SELECT o.openbareruimtenummer                                                                 AS identificatie
     , o.openbareruimtevolgnummer                                                             AS volgnummer
     , o.status_id                                                                            AS status_code
     , s.omschrijving                                                                         AS status_omschrijving
     , to_char(o.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')                                        AS begin_geldigheid
     , to_char(q2.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')                                       AS eind_geldigheid
     , o.indgeconstateerd                                                                     AS geconstateerd
     , o.indinonderzoek                                                                       AS aanduiding_in_onderzoek
     , NULL                                                                                   AS heeft_onderzoeken
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
FROM authentieke_objecten o
    -- begindatum gebruiken als einddatum volgende cyclus
	    JOIN begin_cyclus q1 ON o.openbareruimtenummer = q1.openbareruimtenummer AND
	                            o.openbareruimtevolgnummer = q1.openbareruimtevolgnummer
	    LEFT OUTER JOIN eind_cyclus q2 ON  q1.openbareruimtenummer = q2.openbareruimtenummer AND
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
