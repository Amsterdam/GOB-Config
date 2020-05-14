WITH
    -- Utility functions
    -- Use max_date if eindgeldigheid is NULL
    FUNCTION max_date RETURN char AS
    BEGIN
        RETURN to_date(9999, 'yyyy');
    END;
    -- Determine if a cycle of an objectklasse is in onderzoek
    FUNCTION cyclus_in_onderzoek(
    	begin_cyclus    IN DATE,
    	eind_cyclus     IN DATE,
    	begin_onderzoek IN DATE,
    	eind_onderzoek  IN DATE) RETURN number AS
	BEGIN
       IF (
            -- eindgeldigheid van object is altijd later dan begingeldigheid van onderzoek
            eind_cyclus > begin_onderzoek AND
            -- begingeldigheid van object is altijd eerder dan eindgeldigheid van onderzoek
            begin_cyclus < eind_onderzoek AND
           -- Er dient gekeken te worden naar de gerelateerde objecten bij een eindgeldigheid van een cyclus.
            eind_cyclus <= eind_onderzoek
          )
          OR
          (
            -- begingeldigheid van object is gelijk aan begingeldigheid van onderzoek
            begin_cyclus = begin_onderzoek AND
            -- neem laatste cyclus van object, indien begingeldigheid en geldigheid van object gelijk zijn
            begin_cyclus = eind_cyclus AND
            -- Er dient gekeken te worden naar de gerelateerde objecten bij een eindgeldigheid van een cyclus.
            eind_cyclus <= eind_onderzoek
          )
      	THEN RETURN 1;
        ELSE RETURN 0;
        END IF;
	END;
    -- SubQuery Factoring for onderzoeken
    -- Alle onderzoeken voor deze objectklasse
    in_onderzoeken AS (SELECT /*+ MATERIALIZE */
                              identificatie
                            , versie_identificatie
                            , object_identificatie
                            , inonderzoek
                            , to_date(begin_geldigheid, 'yyyy-mm-dd')                 AS begin_onderzoek
                            , nvl(to_date(eind_geldigheid, 'yyyy-mm-dd'), max_date()) AS eind_onderzoek
                       FROM   lvbag.inonderzoek
                       WHERE  objecttype = 20 ORDER BY object_identificatie),
    -- All onderzoeken gegroepeerd per dag op maximum versie
    -- Onderzoeken die meerdere statussen hebben per dag worden beoordeeld op de status aan het einde van de dag
    in_onderzoeken_eod AS (SELECT /*+ MATERIALIZE */ io.*
            	 	       FROM in_onderzoeken io
                           INNER JOIN (SELECT   identificatie
                                              , begin_onderzoek
                                              , max(versie_identificatie) AS maxversie
                                       FROM     in_onderzoeken
                                       GROUP BY identificatie, begin_onderzoek) io_eod
                           ON io.identificatie = io_eod.identificatie AND
                              io.versie_identificatie = io_eod.maxversie
                           WHERE io.inonderzoek = 'J'),
    -- SubQuery factoring for objectklasse dataset
    authentieke_objecten AS (SELECT *
                             FROM   basis.openbareruimte
                             WHERE  indauthentiek = 'J'),
    -- SubQuery factoring for begin and eindgeldigheid
    -- begindatum gebruiken als einddatum volgende cyclus
    begin_cyclus AS (SELECT openbareruimtenummer
	                      , openbareruimtevolgnummer
	                      , 1 + dense_rank() OVER (partition BY openbareruimtenummer ORDER BY openbareruimtevolgnummer) AS rang
	                 FROM   authentieke_objecten),
    eind_cyclus AS (SELECT openbareruimtenummer
	                     , openbareruimtevolgnummer
	                     , datumopvoer
                         , nvl(trunc(datumopvoer), max_date()) as eind_cyclus
	                     , dense_rank() OVER (partition BY openbareruimtenummer ORDER BY openbareruimtevolgnummer) AS rang
	                FROM   authentieke_objecten),
    -- SubQuery factoring for shared datasets
    adressen AS (SELECT   adres_id
                        , adresnummer
                 FROM     basis.adres
                 WHERE    indauthentiek = 'J'
                 GROUP BY adres_id, adresnummer)
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
        FROM  in_onderzoeken_eod io
        WHERE io.object_identificatie = o.openbareruimtenummer
          AND cyclus_in_onderzoek(trunc(o.datumopvoer), q2.eind_cyclus,
                                  io.begin_onderzoek, io.eind_onderzoek) = 1
       )                                                                                      AS aanduiding_in_onderzoek
     , (SELECT listagg(identificatie, ';')
        FROM   in_onderzoeken io
        WHERE  object_identificatie = o.openbareruimtenummer
          AND  cyclus_in_onderzoek(trunc(o.datumopvoer), q2.eind_cyclus,
                                   io.begin_onderzoek, io.eind_onderzoek) = 1
       )                                                                                      AS heeft_onderzoeken
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
