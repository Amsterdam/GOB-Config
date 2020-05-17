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
                       WHERE  objecttype = 112 ORDER BY object_identificatie),
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
                             FROM   basis.standplaats
                             WHERE  indauthentiek = 'J'),
    -- SubQuery factoring for begin and eindgeldigheid
    -- begindatum gebruiken als einddatum volgende cyclus
    begin_cyclus AS (SELECT standplaatsnummer
	                      , standplaatsvolgnummer
	                      , 1 + dense_rank() OVER (partition BY standplaatsnummer ORDER BY standplaatsvolgnummer) AS rang
	                 FROM   authentieke_objecten),
    eind_cyclus AS (SELECT standplaatsnummer
	                     , standplaatsvolgnummer
	                     , datumopvoer
                         , nvl(trunc(datumopvoer), max_date()) as eind_cyclus
	                     , dense_rank() OVER (partition BY standplaatsnummer ORDER BY standplaatsvolgnummer) AS rang
	                FROM   authentieke_objecten),
    -- SubQuery factoring for shared datasets
    adressen AS (SELECT   adres_id
                        , adresnummer
                 FROM     basis.adres
                 WHERE    indauthentiek = 'J'
                 GROUP BY adres_id, adresnummer)
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
        FROM  in_onderzoeken_eod io
        WHERE io.object_identificatie = s.standplaatsnummer
          AND cyclus_in_onderzoek(trunc(s.datumopvoer), q2.eind_cyclus,
                                  io.begin_onderzoek, io.eind_onderzoek) = 1
       )                                                                                      AS aanduiding_in_onderzoek
     , (SELECT listagg(identificatie, ';')
        FROM   in_onderzoeken io
        WHERE  object_identificatie = s.standplaatsnummer
          AND  cyclus_in_onderzoek(trunc(s.datumopvoer), q2.eind_cyclus,
                                   io.begin_onderzoek, io.eind_onderzoek) = 1
       )                                                                                      AS heeft_onderzoeken
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
FROM authentieke_objecten s
     -- begindatum gebruiken als einddatum volgende cyclus
	    JOIN begin_cyclus q1 ON s.standplaatsnummer = q1.standplaatsnummer AND
	                            s.standplaatsvolgnummer = q1.standplaatsvolgnummer
	    LEFT OUTER JOIN eind_cyclus q2 ON  q1.standplaatsnummer = q2.standplaatsnummer AND
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
                                   JOIN adressen a ON a.adres_id = sa.adres_id
                          WHERE sa.indhoofdadres = 'J') q1 ON s.standplaats_id = q1.standplaats_id AND
                                                              s.standplaatsvolgnummer = q1.standplaatsvolgnummer
    -- selecteren nevenadres(sen)
         LEFT OUTER JOIN (SELECT sa.standplaats_id
                               , sa.standplaatsvolgnummer
                               , listagg(a.adresnummer, ';')
                                 WITHIN GROUP (ORDER BY sa.standplaats_id,sa.standplaatsvolgnummer) AS adresnummer
                          FROM basis.standplaats_adres sa
                                   JOIN adressen a ON a.adres_id = sa.adres_id
                          WHERE sa.indhoofdadres = 'N'
                          GROUP BY sa.standplaats_id, sa.standplaatsvolgnummer) q2 ON s.standplaats_id = q2.standplaats_id AND
                                                                                      s.standplaatsvolgnummer = q2.standplaatsvolgnummer
