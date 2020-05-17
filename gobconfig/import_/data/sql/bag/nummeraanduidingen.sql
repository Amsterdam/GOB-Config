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
                             FROM   basis.adres
                             WHERE  indauthentiek = 'J'),
    -- SubQuery factoring for begin and eindgeldigheid
    -- begindatum gebruiken als einddatum volgende cyclus
    begin_cyclus AS (SELECT adresnummer
	                      , adresvolgnummer
                          , datumopvoer
	                      , 1 + dense_rank() OVER (partition BY adresnummer ORDER BY adresvolgnummer) AS rang
	                 FROM   authentieke_objecten),
    eind_cyclus AS (SELECT adresnummer
	                     , adresvolgnummer
	                     , datumopvoer
	                     , dense_rank() OVER (partition BY adresnummer ORDER BY adresvolgnummer) AS rang
	                FROM   authentieke_objecten),
 	cyclus AS (SELECT bc.adresnummer                         AS object_nummer
	                , bc.adresvolgnummer                     AS object_volgnummer
	                , trunc(bc.datumopvoer)                  AS begin_cyclus
	                , nvl(trunc(ec.datumopvoer), max_date()) AS eind_cyclus
	           FROM begin_cyclus bc
	           LEFT OUTER JOIN eind_cyclus ec ON  bc.adresnummer = ec.adresnummer AND
							                      bc.rang = ec.rang),
    -- SubQuery Factoring for onderzoeken
    -- All onderzoeken gegroepeerd per onderzoek per object per dag op de toestand aan het einde van de dag
    inonderzoeken_per_dag AS (SELECT identificatie
                                   , object_identificatie
                                   , begin_geldigheid
                                   , max(versie_identificatie) AS eodversie
                              FROM   lvbag.inonderzoek
                              WHERE  objecttype = 21
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
                                    , ao.adresvolgnummer AS object_volgnummer
                                    , io.inonderzoek
                                    , io.begin_onderzoek
                                    , io.eind_onderzoek
                               FROM   in_onderzoeken io
                               INNER JOIN authentieke_objecten ao ON io.object_identificatie = ao.adresnummer
                               JOIN cyclus c ON ao.adresnummer = c.object_nummer AND
							                    ao.adresvolgnummer = c.object_volgnummer
                               WHERE cyclus_in_onderzoek(c.begin_cyclus, c.eind_cyclus,
                                                         io.begin_onderzoek, io.eind_onderzoek) = 1
                               ORDER BY io.object_identificatie, ao.adresvolgnummer
                               )
SELECT      a.adresnummer                                                                          AS identificatie
     ,      a.adresvolgnummer                                                                      AS volgnummer
     ,      a.statuscode                                                                           AS status_code
     ,      s.omschrijving                                                                         AS status_omschrijving
     ,      a.huisnummer                                                                           AS huisnummer
     ,      a.huisletter                                                                           AS huisletter
     ,      a.toevoeging                                                                           AS huisnummertoevoeging
     ,      a.postcode_num || a.postcode_alf                                                       AS postcode
     ,      a.indgeconstateerd                                                                     AS geconstateerd
     ,      q1.openbareruimtenummer                                                                AS ligt_aan_bag_openbareruimte
     ,      to_char(a.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')                                        AS begin_geldigheid
     ,      to_char(q2.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')                                       AS eind_geldigheid
     , (SELECT CASE
               WHEN listagg(inonderzoek, ';') LIKE '%J%'
               THEN 'J'
               ELSE 'N'
               END
        FROM   effectieve_onderzoeken io
        WHERE  io.object_identificatie = a.adresnummer AND io.object_volgnummer = a.adresvolgnummer
       )                                                                                           AS aanduiding_in_onderzoek
     , (SELECT listagg(identificatie, ';')
        FROM   effectieve_onderzoeken io
        WHERE  io.object_identificatie = a.adresnummer AND io.object_volgnummer = a.adresvolgnummer
       )                                                                                           AS heeft_onderzoeken
     ,      a.adrestype                                                                            AS type_aot_code
     ,      t.omschrijving                                                                         AS type_aot_omschrijving
     ,      to_char(a.dd_document, 'YYYY-MM-DD')                                                   AS documentdatum
     ,      a.documentnummer                                                                       AS documentnummer
     ,      TRIM(nvl(regexp_substr(a.documentnummer, '^(.*?)_', 1, 1, NULL, 1), a.documentnummer)) AS dossier
     ,      a.bagproces                                                                            AS bagproces_code
     ,      m.omschrijving                                                                         AS bagproces_omschrijving
     ,      CASE q2.indhoofdadres
            WHEN 'J' THEN 'Hoofdadres'
            WHEN 'N' THEN 'Nevenadres'
            END                                                                                    AS type_adres
     ,      q3.verblijfseenheidnummer                                                              AS adresseert_bag_verblijfsobject
     ,      q4.ligplaatsnummer                                                                     AS adresseert_bag_ligplaats
     ,      q5.standplaatsnummer                                                                   AS adresseert_bag_standplaats
     ,      to_char(a.creation, 'YYYY-MM-DD HH24:MI:SS')                                           AS registratiedatum
     ,      a.adres_id                                                                             AS source_id
     ,      NVL2(q2.datumopvoer,
                 CASE
                 WHEN q2.datumopvoer < sysdate
                 THEN to_char(q2.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')
                 ELSE to_char(a.modification, 'YYYY-MM-DD HH24:MI:SS')
                 END
    , CASE WHEN s.status = 2
           THEN
               CASE
               WHEN q2.datumopvoer < sysdate
               THEN to_char(a.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')
               ELSE to_char(a.creation, 'YYYY-MM-DD HH24:MI:SS')
               END
      ELSE NULL
      END)                                                                                   AS expirationdate
     , CASE
       WHEN (INSTR(q3.ligt_in_woonplaats, '1024;3594') > 0 OR INSTR(q3.ligt_in_woonplaats, '1025;3594') > 0)
            AND q2.datumopvoer < DATE '2014-01-10'
       THEN REGEXP_SUBSTR(q3.ligt_in_woonplaats, '[^;]+', 1, 1)
       WHEN (INSTR(q3.ligt_in_woonplaats, '1024;3594') > 0 OR INSTR(q3.ligt_in_woonplaats, '1025;3594') > 0)
            AND (q2.datumopvoer >= DATE '2014-01-10' OR q2.datumopvoer IS NULL)
       THEN REGEXP_SUBSTR(q3.ligt_in_woonplaats, '[^;]+', 1, 2)
       ELSE q3.ligt_in_woonplaats
       END                                                                                         AS ligt_in_bag_woonplaats
FROM authentieke_objecten a
     -- begindatum gebruiken als einddatum volgende cyclus
	    JOIN begin_cyclus q1 ON a.adresnummer = q1.adresnummer AND
	                            a.adresvolgnummer = q1.adresvolgnummer
	    LEFT OUTER JOIN eind_cyclus q2 ON  q1.adresnummer = q2.adresnummer AND
	                                       q1.rang = q2.rang
     -- selecteren openbare ruimte
           LEFT OUTER JOIN (SELECT DISTINCT o.openbareruimte_id
                                          ,      o.openbareruimtenummer
                            FROM   basis.openbareruimte o
                            WHERE  o.indauthentiek = 'J') q1 ON a.openbareruimte_id = q1.openbareruimte_id
    -- selecteren woonplaats
           LEFT OUTER JOIN (SELECT w2.openbareruimte_id
                                 , listagg(w2.woonplaatsnummer, ';') WITHIN GROUP (ORDER BY w2.woonplaatsnummer) AS ligt_in_woonplaats
                            FROM  (SELECT o.openbareruimte_id
                                        , w.woonplaats_id
                                        , w.woonplaatsnummer
                                   FROM   basis.openbareruimte o
                                   JOIN   basis.woonplaats     w ON o.woonplaats_id = w.woonplaats_id
                                   WHERE  o.indauthentiek = 'J'
                                     AND  w.indauthentiek = 'J'
                                   GROUP BY o.openbareruimte_id
                                          , w.woonplaats_id
                                          , w.woonplaatsnummer) w2
                            GROUP BY w2.openbareruimte_id) q3 ON  q1.openbareruimte_id = q3.openbareruimte_id
    -- selecteren type adresseerbaar object
           LEFT OUTER JOIN basis.adrestype t ON a.adrestype = t.adrestype
    -- selecteren status
           LEFT OUTER JOIN basis.adresstatus s ON a.statuscode = s.status
    -- selecteren bagproces / mutatiereden
           LEFT OUTER JOIN basis.mutatiereden m ON a.bagproces = m.id
    -- selecteren hoofdadres/nevenadres (hoofdadres igv hoofdadres+nevenadres)
           LEFT OUTER JOIN (SELECT adres_id
                                 , MIN(indhoofdadres) AS indhoofdadres
                            FROM   basis.verblijfseenheid_adres
                            GROUP BY adres_id
                            UNION
                            SELECT adres_id
                                 , MIN(indhoofdadres) AS indhoofdadres
                            FROM   basis.ligplaats_adres
                            GROUP BY adres_id
                            UNION
                            SELECT adres_id
                                 , MIN(indhoofdadres) AS indhoofdadres
                            FROM   basis.standplaats_adres
                            GROUP BY adres_id) q2 ON a.adres_id = q2.adres_id
    -- selecteren verblijfsobject
           LEFT OUTER JOIN (SELECT va.verblijfseenheid_id
                                 , va.adres_id
                                 , ve.verblijfseenheidnummer
                            FROM   basis.verblijfseenheid_adres  va
                            JOIN   basis.verblijfseenheid        ve
                                ON  va.verblijfseenheid_id = ve.verblijfseenheid_id AND
                                    va.verblijfseenheidvolgnummer = ve.verblijfseenheidvolgnummer
                            WHERE  ve.indauthentiek = 'J'
                            GROUP BY va.verblijfseenheid_id
                                   , va.adres_id
                                   , ve.verblijfseenheidnummer) q3 ON a.adres_id = q3.adres_id
    -- selecteren ligplaats
           LEFT OUTER JOIN (SELECT la.ligplaats_id
                                 , la.adres_id
                                 , lp.ligplaatsnummer
                            FROM   basis.ligplaats_adres la
                            JOIN   basis.ligplaats       lp ON  la.ligplaats_id = lp.ligplaats_id AND
                                                                la.ligplaatsvolgnummer = lp.ligplaatsvolgnummer
                            WHERE  lp.indauthentiek = 'J'
                            GROUP BY la.ligplaats_id
                                   , la.adres_id
                                   , lp.ligplaatsnummer) q4 ON a.adres_id = q4.adres_id
    -- selecteren standplaats
           LEFT OUTER JOIN (SELECT sa.standplaats_id
                                 , sa.adres_id
                                 , sp.standplaatsnummer
                            FROM   basis.standplaats_adres sa
                            JOIN   basis.standplaats       sp ON  sa.standplaats_id = sp.standplaats_id AND
                                                                  sa.standplaatsvolgnummer = sp.standplaatsvolgnummer
                            WHERE  sp.indauthentiek = 'J'
                            GROUP BY sa.standplaats_id
                                   , sa.adres_id
                                   , sp.standplaatsnummer) q5 ON a.adres_id = q5.adres_id
