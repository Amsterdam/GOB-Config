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
                             FROM   basis.gebouw
                             WHERE  indauthentiek = 'J'),
    -- SubQuery factoring for begin and eindgeldigheid
    -- begindatum gebruiken als einddatum volgende cyclus
    begin_cyclus AS (SELECT gebouwnummer
	                      , gebouwvolgnummer
                          , datumopvoer
	                      , 1 + dense_rank() OVER (partition BY gebouwnummer ORDER BY gebouwvolgnummer) AS rang
	                 FROM   authentieke_objecten),
    eind_cyclus AS (SELECT gebouwnummer
	                     , gebouwvolgnummer
	                     , datumopvoer
	                     , dense_rank() OVER (partition BY gebouwnummer ORDER BY gebouwvolgnummer) AS rang
	                FROM   authentieke_objecten),
 	cyclus AS (SELECT bc.gebouwnummer                        AS object_nummer
	                , bc.gebouwvolgnummer                    AS object_volgnummer
	                , trunc(bc.datumopvoer)                  AS begin_cyclus
	                , nvl(trunc(ec.datumopvoer), max_date()) AS eind_cyclus
	           FROM begin_cyclus bc
	           LEFT OUTER JOIN eind_cyclus ec ON  bc.gebouwnummer = ec.gebouwnummer AND
							                      bc.rang = ec.rang),
    -- SubQuery Factoring for onderzoeken
    -- All onderzoeken gegroepeerd per onderzoek per object per dag op de toestand aan het einde van de dag
    inonderzoeken_per_dag AS (SELECT identificatie
                                   , object_identificatie
                                   , begin_geldigheid
                                   , max(versie_identificatie) AS eodversie
                              FROM   lvbag.inonderzoek
                              WHERE  objecttype = 101
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
                                    , ao.gebouwvolgnummer AS object_volgnummer
                                    , io.inonderzoek
                                    , io.begin_onderzoek
                                    , io.eind_onderzoek
                               FROM   in_onderzoeken io
                               INNER JOIN authentieke_objecten ao ON io.object_identificatie = ao.gebouwnummer
                               JOIN cyclus c ON ao.gebouwnummer = c.object_nummer AND
							                    ao.gebouwvolgnummer = c.object_volgnummer
                               WHERE cyclus_in_onderzoek(c.begin_cyclus, c.eind_cyclus,
                                                         io.begin_onderzoek, io.eind_onderzoek) = 1
                               ORDER BY io.object_identificatie, ao.gebouwvolgnummer
                               )
SELECT g.gebouwnummer                                                                         AS identificatie
     , g.gebouwvolgnummer                                                                     AS volgnummer
     , g.indgeconstateerd                                                                     AS geconstateerd
     , to_char(g.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')                                        AS begin_geldigheid
     , to_char(q2.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')                                       AS eind_geldigheid
     , (SELECT CASE
               WHEN listagg(inonderzoek, ';') LIKE '%J%'
               THEN 'J'
               ELSE 'N'
               END
        FROM   effectieve_onderzoeken io
        WHERE  io.object_identificatie = g.gebouwnummer AND io.object_volgnummer = g.gebouwvolgnummer
       )                                                                                      AS aanduiding_in_onderzoek
     , (SELECT listagg(identificatie, ';')
        FROM   effectieve_onderzoeken io
        WHERE  io.object_identificatie = g.gebouwnummer AND io.object_volgnummer = g.gebouwvolgnummer
       )                                                                                      AS heeft_onderzoeken
     , g.status_id                                                                            AS status_code
     , s.omschrijving                                                                         AS status_omschrijving
     , CASE
       WHEN y.aantal_wonen = 1
       THEN 'Eén woning'
       WHEN y.aantal_wonen > 1
       THEN 'Meerdere woningen'
       END                                                                                    AS type_woonobject
     , ROUND(g.aantal_bouwlagen)                                                              AS aantal_bouwlagen
     , ROUND(g.laagste_bouwlaag)                                                              AS laagste_bouwlaag
     , ROUND(g.hoogste_bouwlaag)                                                              AS hoogste_bouwlaag
     , g.gebouwtype                                                                           AS ligging_code
     , t.omschrijving                                                                         AS ligging_omschrijving
     , g.naam                                                                                 AS naam
     , g.bouwjaar                                                                             AS oorspronkelijk_bouwjaar
     , sdo_util.to_wktgeometry(g.geometrie)                                                   AS geometrie
     , to_char(g.dd_document, 'YYYY-MM-DD')                                                   AS documentdatum
     , g.documentnummer                                                                       AS documentnummer
     , TRIM(nvl(regexp_substr(g.documentnummer, '^(.*?)_', 1, 1, NULL, 1), g.documentnummer)) AS dossier
     , g.bagproces                                                                            AS bagproces_code
     , m.omschrijving                                                                         AS bagproces_omschrijving
     , to_char(g.creation, 'YYYY-MM-DD HH24:MI:SS')                                           AS registratiedatum
     , g.gebouw_id                                                                            AS source_id
     , NVL2(q2.datumopvoer,
            CASE
            WHEN q2.datumopvoer < sysdate
            THEN to_char(q2.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')
            ELSE to_char(g.modification, 'YYYY-MM-DD HH24:MI:SS')
            END
    , CASE
      WHEN s.status NOT IN (1, 2, 3, 7, 10, 11, 12, 13)
      THEN CASE
           WHEN q2.datumopvoer < sysdate
           THEN to_char(g.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')
           ELSE to_char(g.creation, 'YYYY-MM-DD HH24:MI:SS')
           END
      ELSE NULL
      END)                                                                                    AS expirationdate
FROM authentieke_objecten g
     -- begindatum gebruiken als einddatum volgende cyclus
	    JOIN begin_cyclus q1 ON g.gebouwnummer = q1.gebouwnummer AND
	                            g.gebouwvolgnummer = q1.gebouwvolgnummer
	    LEFT OUTER JOIN eind_cyclus q2 ON  q1.gebouwnummer = q2.gebouwnummer AND
	                                       q1.rang = q2.rang
    -- selecteren status
         LEFT OUTER JOIN basis.gebouwstatus s ON g.status_id = s.status
    -- selecteren bagproces / mutatiereden
         LEFT OUTER JOIN basis.mutatiereden m ON g.bagproces = m.id
    -- selecteren ligging
         LEFT OUTER JOIN basis.gebouwtype t ON g.gebouwtype = t.gebouwtype
    -- selecteren type woonobject
         LEFT OUTER JOIN (SELECT x1.gebouw_id
                               , x1.gebouwvolgnummer
                               , COUNT(CASE
                                       WHEN x2.gebruiksdoel_id > 1
                                       THEN NULL
                                       ELSE x2.gebruiksdoel_id
                                       END) AS aantal_wonen
                               , COUNT(CASE
                                       WHEN x2.gebruiksdoel_id = 1
                                       THEN NULL
                                       ELSE x2.gebruiksdoel_id
                                       END) AS aantal_nietwonen
                          FROM (SELECT vg.gebouw_id
                                     , vg.gebouwvolgnummer
                                     , vg.verblijfseenheid_id
                                     , MAX(vg.verblijfseenheidvolgnummer) AS verblijfseenheidvolgnummer
                                FROM basis.verblijfseenheid_gebouw vg
                                GROUP BY vg.gebouw_id
                                       , vg.gebouwvolgnummer
                                       , vg.verblijfseenheid_id) x1
                                   JOIN (SELECT vg.verblijfsobject_id
                                              , vg.verblijfsobjectvolgnummer
                                              , vg.gebruiksdoel_id
                                         FROM basis.verblijfsobject_gebruiksdoel vg) x2 ON x1.verblijfseenheid_id = x2.verblijfsobject_id AND
                                                                                           x1.verblijfseenheidvolgnummer = x2.verblijfsobjectvolgnummer
                          GROUP BY x1.gebouw_id, x1.gebouwvolgnummer) y ON y.gebouw_id = g.gebouw_id
    AND y.gebouwvolgnummer = g.gebouwvolgnummer
