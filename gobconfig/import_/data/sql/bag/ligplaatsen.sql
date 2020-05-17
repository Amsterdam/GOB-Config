WITH
    -- Utility functions
    -- Use max_date if eindgeldigheid is NULL
    FUNCTION max_date RETURN DATE AS
    BEGIN
        RETURN to_date('9999', 'yyyy');
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
    -- SubQuery factoring for objectklasse dataset
    authentieke_objecten AS (SELECT *
                             FROM   basis.ligplaats
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
	cyclus AS (SELECT bc.ligplaatsnummer                     AS object_nummer
	                , bc.ligplaatsvolgnummer                 AS object_volgnummer
	                , trunc(bc.datumopvoer)                  AS begin_cyclus
	                , nvl(trunc(ec.datumopvoer), max_date()) AS eind_cyclus
	           FROM begin_cyclus bc
	           LEFT OUTER JOIN eind_cyclus ec ON  bc.ligplaatsnummer = ec.ligplaatsnummer AND
							                      bc.rang = ec.rang),
    -- SubQuery Factoring for onderzoeken
    -- All onderzoeken gegroepeerd per onderzoek per object per dag op de toestand aan het einde van de dag
    inonderzoeken_per_dag AS (SELECT identificatie
                                   , object_identificatie
                                   , begin_geldigheid
                                   , max(versie_identificatie) AS eodversie
                              FROM   lvbag.inonderzoek
                              WHERE  objecttype = 111
                              GROUP BY identificatie, object_identificatie, begin_geldigheid),
    in_onderzoeken AS (SELECT io.identificatie
                            , io.versie_identificatie
                            , io.object_identificatie
                            , io.inonderzoek
                            , to_date(io.begin_geldigheid, 'yyyy-mm-dd')                 AS begin_onderzoek
                            , nvl(to_date(io.eind_geldigheid, 'yyyy-mm-dd'), max_date()) AS eind_onderzoek
                       FROM   lvbag.inonderzoek io
                       INNER JOIN inonderzoeken_per_dag io_pd
                               ON io.identificatie = io_pd.identificatie AND
                                  io.versie_identificatie = io_pd.eodversie),
    effectieve_onderzoeken AS (SELECT /*+ MATERIALIZE */
                                      io.identificatie
                                    , io.object_identificatie
                                    , ao.ligplaatsvolgnummer AS object_volgnummer
                                    , io.inonderzoek
                                    , io.begin_onderzoek
                                    , io.eind_onderzoek
                               FROM   in_onderzoeken io
                               INNER JOIN authentieke_objecten ao ON io.object_identificatie = ao.ligplaatsnummer
                               JOIN cyclus c ON ao.ligplaatsnummer = c.object_nummer AND
							                    ao.ligplaatsvolgnummer = c.object_volgnummer
                               WHERE cyclus_in_onderzoek(c.begin_cyclus, c.eind_cyclus,
                                                         io.begin_onderzoek, io.eind_onderzoek) = 1
                               ),
    -- SubQuery factoring for shared datasets
    adressen AS (SELECT   adres_id
                        , adresnummer
                 FROM     basis.adres
                 WHERE    indauthentiek = 'J'
                 GROUP BY adres_id, adresnummer)
SELECT l.ligplaatsnummer                                                                      AS identificatie
     , l.ligplaatsvolgnummer                                                                  AS volgnummer
     , l.indgeconstateerd                                                                     AS geconstateerd
     , to_char(l.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')                                        AS begin_geldigheid
     , to_char(q2.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')                                       AS eind_geldigheid
     , (SELECT CASE
               WHEN listagg(inonderzoek, ';') LIKE '%J%'
               THEN 'J'
               ELSE 'N'
               END
        FROM   effectieve_onderzoeken io
        WHERE  io.object_identificatie = l.ligplaatsnummer AND io.object_volgnummer = l.ligplaatsvolgnummer
       )                                                                                      AS aanduiding_in_onderzoek
     , (SELECT listagg(identificatie, ';')
        FROM   effectieve_onderzoeken io
        WHERE  io.object_identificatie = l.ligplaatsnummer AND io.object_volgnummer = l.ligplaatsvolgnummer
       )                                                                                      AS heeft_onderzoeken
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
FROM authentieke_objecten l
     -- begindatum gebruiken als einddatum volgende cyclus
	    JOIN begin_cyclus q1 ON l.ligplaatsnummer = q1.ligplaatsnummer AND
	                            l.ligplaatsvolgnummer = q1.ligplaatsvolgnummer
	    LEFT OUTER JOIN eind_cyclus q2 ON  q1.ligplaatsnummer = q2.ligplaatsnummer AND
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
                                   JOIN adressen a ON a.adres_id = la.adres_id
                          WHERE la.indhoofdadres = 'J') q1 ON l.ligplaats_id = q1.ligplaats_id AND
                                                              l.ligplaatsvolgnummer = q1.ligplaatsvolgnummer
    -- selecteren nevenadres(sen)
         LEFT OUTER JOIN (SELECT la.ligplaats_id
                               , la.ligplaatsvolgnummer
                               , listagg(a.adresnummer, ';')
                                 WITHIN GROUP (ORDER BY la.ligplaats_id,la.ligplaatsvolgnummer) AS adresnummer
                          FROM basis.ligplaats_adres la
                                   JOIN adressen a ON a.adres_id = la.adres_id
                          WHERE la.indhoofdadres = 'N'
                          GROUP BY la.ligplaats_id, la.ligplaatsvolgnummer) q2 ON l.ligplaats_id = q2.ligplaats_id AND
                                                                                  l.ligplaatsvolgnummer = q2.ligplaatsvolgnummer
