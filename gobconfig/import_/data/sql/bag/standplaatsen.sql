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
                             FROM   basis.standplaats
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
 	cyclus AS (SELECT bc.standplaatsnummer                   AS object_nummer
	                , bc.standplaatsvolgnummer               AS object_volgnummer
	                , trunc(bc.datumopvoer)                  AS begin_cyclus
	                , nvl(trunc(ec.datumopvoer), max_date()) AS eind_cyclus
	           FROM begin_cyclus bc
	           LEFT OUTER JOIN eind_cyclus ec ON  bc.standplaatsnummer = ec.standplaatsnummer AND
							                      bc.rang = ec.rang),
    -- SubQuery Factoring for onderzoeken
    -- All onderzoeken gegroepeerd per onderzoek per object per dag op de toestand aan het einde van de dag
    inonderzoeken_per_dag AS (SELECT identificatie
                                   , object_identificatie
                                   , begin_geldigheid
                                   , max(versie_identificatie) AS eodversie
                              FROM   lvbag.inonderzoek
                              WHERE  objecttype = 112
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
                                    , ao.standplaatsvolgnummer AS object_volgnummer
                                    , io.inonderzoek
                                    , io.begin_onderzoek
                                    , io.eind_onderzoek
                               FROM   in_onderzoeken io
                               INNER JOIN authentieke_objecten ao ON io.object_identificatie = ao.standplaatsnummer
                               JOIN cyclus c ON ao.standplaatsnummer = c.object_nummer AND
							                    ao.standplaatsvolgnummer = c.object_volgnummer
                               WHERE cyclus_in_onderzoek(c.begin_cyclus, c.eind_cyclus,
                                                         io.begin_onderzoek, io.eind_onderzoek) = 1
                               ORDER BY io.object_identificatie, ao.standplaatsvolgnummer
                               ),
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
               WHEN listagg(inonderzoek, ';') LIKE '%J%'
               THEN 'J'
               ELSE 'N'
               END
        FROM   effectieve_onderzoeken io
        WHERE  io.object_identificatie = s.standplaatsnummer AND io.object_volgnummer = s.standplaatsvolgnummer
       )                                                                                      AS aanduiding_in_onderzoek
     , (SELECT listagg(identificatie, ';')
        FROM   effectieve_onderzoeken io
        WHERE  io.object_identificatie = s.standplaatsnummer AND io.object_volgnummer = s.standplaatsvolgnummer
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
