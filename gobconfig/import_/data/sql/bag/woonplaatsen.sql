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
	                FROM   authentieke_objecten),
 	cyclus AS (SELECT bc.woonplaatsnummer                    AS object_nummer
	                , bc.woonplaatsvolgnummer                AS object_volgnummer
	                , trunc(bc.datumopvoer)                  AS begin_cyclus
	                , nvl(trunc(ec.datumopvoer), max_date()) AS eind_cyclus
	           FROM begin_cyclus bc
	           LEFT OUTER JOIN eind_cyclus ec ON  bc.woonplaatsnummer = ec.woonplaatsnummer AND
							                      bc.rang = ec.rang),
    -- SubQuery Factoring for onderzoeken
    -- All onderzoeken gegroepeerd per onderzoek per object per dag op de toestand aan het einde van de dag
    inonderzoeken_per_dag AS (SELECT identificatie
                                   , object_identificatie
                                   , begin_geldigheid
                                   , max(versie_identificatie) AS eodversie
                              FROM   lvbag.inonderzoek
                              WHERE  objecttype = 113
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
                                    , ao.woonplaatsvolgnummer AS object_volgnummer
                                    , io.inonderzoek
                                    , io.begin_onderzoek
                                    , io.eind_onderzoek
                               FROM   in_onderzoeken io
                               INNER JOIN authentieke_objecten ao ON io.object_identificatie = ao.woonplaatsnummer
                               JOIN cyclus c ON ao.woonplaatsnummer = c.object_nummer AND
							                    ao.woonplaatsvolgnummer = c.object_volgnummer
                               WHERE cyclus_in_onderzoek(c.begin_cyclus, c.eind_cyclus,
                                                         io.begin_onderzoek, io.eind_onderzoek) = 1
                               ORDER BY io.object_identificatie, ao.woonplaatsvolgnummer
                               )
SELECT w.woonplaatsnummer                                                                     AS identificatie
     , w.woonplaatsvolgnummer                                                                 AS volgnummer
     , s.status                                                                               AS status_code
     , s.omschrijving                                                                         AS status_omschrijving
     , to_char(w.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')                                        AS begin_geldigheid
     , to_char(q2.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')                                       AS eind_geldigheid
     , (SELECT CASE
               WHEN listagg(inonderzoek, ';') LIKE '%J%'
               THEN 'J'
               ELSE 'N'
               END
        FROM   effectieve_onderzoeken io
        WHERE  io.object_identificatie = w.woonplaatsnummer AND io.object_volgnummer = w.woonplaatsvolgnummer
       )                                                                                      AS aanduiding_in_onderzoek
     , (SELECT listagg(identificatie, ';')
        FROM   effectieve_onderzoeken io
        WHERE  io.object_identificatie = w.woonplaatsnummer AND io.object_volgnummer = w.woonplaatsvolgnummer
       )                                                                                      AS heeft_onderzoeken
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
