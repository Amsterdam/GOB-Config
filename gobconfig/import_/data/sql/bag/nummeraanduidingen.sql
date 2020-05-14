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
                       WHERE  objecttype = 21 ORDER BY object_identificatie),
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
                             FROM   basis.adres
                             WHERE  indauthentiek = 'J'),
    -- SubQuery factoring for begin and eindgeldigheid
    -- begindatum gebruiken als einddatum volgende cyclus
    begin_cyclus AS (SELECT adresnummer
	                      , adresvolgnummer
	                      , 1 + dense_rank() OVER (partition BY adresnummer ORDER BY adresvolgnummer) AS rang
	                 FROM   authentieke_objecten),
    eind_cyclus AS (SELECT adresnummer
	                     , adresvolgnummer
	                     , datumopvoer
                         , nvl(trunc(datumopvoer), max_date()) as eind_cyclus
	                     , dense_rank() OVER (partition BY adresnummer ORDER BY adresvolgnummer) AS rang
	                FROM   authentieke_objecten),
    -- SubQuery factoring for shared datasets
    adressen AS (SELECT   adres_id
                        , adresnummer
                 FROM     basis.adres
                 WHERE    indauthentiek = 'J'
                 GROUP BY adres_id, adresnummer)
SELECT a.adresnummer                                                                          AS identificatie
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
               WHEN listagg(inonderzoek, ';') WITHIN GROUP (ORDER BY object_identificatie) LIKE '%J%'
               THEN 'J'
               ELSE 'N'
               END
        FROM  in_onderzoeken_eod io
        WHERE io.object_identificatie = a.adresnummer
          AND cyclus_in_onderzoek(trunc(a.datumopvoer), q2.eind_cyclus,
                                  io.begin_onderzoek, io.eind_onderzoek) = 1
       )                                                                                      AS aanduiding_in_onderzoek
     , (SELECT listagg(identificatie, ';')
        FROM   in_onderzoeken io
        WHERE  object_identificatie = a.adresnummer
          AND  cyclus_in_onderzoek(trunc(a.datumopvoer), q2.eind_cyclus,
                                   io.begin_onderzoek, io.eind_onderzoek) = 1
       )                                                                                      AS heeft_onderzoeken
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
