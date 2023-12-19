WITH
    -- SubQuery factoring for objectklasse dataset
    authentieke_objecten AS (SELECT *
                             FROM   G0363_Basis.adres
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
	                FROM   authentieke_objecten)
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
     ,      NULL                                                                                   AS heeft_onderzoeken
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
     , CASE
         -- no endvalidity, use beginvalidity for certain status
         WHEN q2.datumopvoer IS NULL
         THEN
             CASE
                 -- when status = 2, the verblijfsobject is expired at begin_geldigheid
                 WHEN s.status = 2
                 THEN to_char(a.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')
             END
          -- endvalidity exists
         ELSE
             CASE
                 WHEN q2.datumopvoer < sysdate
                 THEN to_char(q2.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')
                 ELSE to_char(a.modification, 'YYYY-MM-DD HH24:MI:SS')
             END
       END                                                                                         AS expirationdate
     , CASE
         -- Amsterdam
         -- Woonplaatsnummer: Before 2014-01-10: 1025, after 3594 (merge with Zuidoost)
         -- https://www.amsterdam.nl/stelselpedia/bag-index/catalogus-bag/objectklasse-woonplaats/
           WHEN
               (INSTR(q3.ligt_in_woonplaats, '1024;3594') > 0 OR INSTR(q3.ligt_in_woonplaats, '1025;3594') > 0)
                AND q2.datumopvoer < DATE '2014-01-10'
               THEN
                    REGEXP_SUBSTR(q3.ligt_in_woonplaats, '[^;]+', 1, 1)
           WHEN
               (INSTR(q3.ligt_in_woonplaats, '1024;3594') > 0 OR INSTR(q3.ligt_in_woonplaats, '1025;3594') > 0)
                   AND (q2.datumopvoer >= DATE '2014-01-10' OR q2.datumopvoer IS NULL)
               THEN
                    REGEXP_SUBSTR(q3.ligt_in_woonplaats, '[^;]+', 1, 2)
        -- Weesp
        -- Woonplaatsnummer: Before 2016-01-01: 1012, after 3631
        -- On 1-1-2016 the gemeente Gooise Meren van Muiden, Naarden en Bussum was created, this changed some borders
           WHEN q3.ligt_in_woonplaats = '1012;3631' AND q2.datumopvoer < DATE '2016-01-01'
                THEN '1012'
           WHEN q3.ligt_in_woonplaats = '1012;3631' AND (q2.datumopvoer >= DATE '2016-01-01' OR q2.datumopvoer IS NULL)
                THEN '3631'
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
                            FROM   G0363_Basis.openbareruimte o
                            WHERE  o.indauthentiek = 'J') q1 ON a.openbareruimte_id = q1.openbareruimte_id
    -- selecteren woonplaats
           LEFT OUTER JOIN (SELECT w2.openbareruimte_id
                                 , listagg(w2.woonplaatsnummer, ';') WITHIN GROUP (ORDER BY w2.woonplaatsnummer) AS ligt_in_woonplaats
                            FROM  (SELECT o.openbareruimte_id
                                        , w.woonplaats_id
                                        , w.woonplaatsnummer
                                   FROM   G0363_Basis.openbareruimte o
                                   JOIN   G0363_Basis.woonplaats     w ON o.woonplaats_id = w.woonplaats_id
                                   WHERE  o.indauthentiek = 'J'
                                     AND  w.indauthentiek = 'J'
                                   GROUP BY o.openbareruimte_id
                                          , w.woonplaats_id
                                          , w.woonplaatsnummer) w2
                            GROUP BY w2.openbareruimte_id) q3 ON  q1.openbareruimte_id = q3.openbareruimte_id
    -- selecteren type adresseerbaar object
           LEFT OUTER JOIN G0363_Basis.adrestype t ON a.adrestype = t.adrestype
    -- selecteren status
           LEFT OUTER JOIN G0363_Basis.adresstatus s ON a.statuscode = s.status
    -- selecteren bagproces / mutatiereden
           LEFT OUTER JOIN G0363_Basis.mutatiereden m ON a.bagproces = m.id
    -- selecteren hoofdadres/nevenadres (hoofdadres igv hoofdadres+nevenadres)
           LEFT OUTER JOIN (SELECT adres_id
                                 , MIN(indhoofdadres) AS indhoofdadres
                            FROM   G0363_Basis.verblijfseenheid_adres
                            GROUP BY adres_id
                            UNION
                            SELECT adres_id
                                 , MIN(indhoofdadres) AS indhoofdadres
                            FROM   G0363_Basis.ligplaats_adres
                            GROUP BY adres_id
                            UNION
                            SELECT adres_id
                                 , MIN(indhoofdadres) AS indhoofdadres
                            FROM   G0363_Basis.standplaats_adres
                            GROUP BY adres_id) q2 ON a.adres_id = q2.adres_id
    -- selecteren verblijfsobject
           LEFT OUTER JOIN (SELECT va.verblijfseenheid_id
                                 , va.adres_id
                                 , ve.verblijfseenheidnummer
                            FROM   G0363_Basis.verblijfseenheid_adres  va
                            JOIN   G0363_Basis.verblijfseenheid        ve
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
                            FROM   G0363_Basis.ligplaats_adres la
                            JOIN   G0363_Basis.ligplaats       lp ON  la.ligplaats_id = lp.ligplaats_id AND
                                                                la.ligplaatsvolgnummer = lp.ligplaatsvolgnummer
                            WHERE  lp.indauthentiek = 'J'
                            GROUP BY la.ligplaats_id
                                   , la.adres_id
                                   , lp.ligplaatsnummer) q4 ON a.adres_id = q4.adres_id
    -- selecteren standplaats
           LEFT OUTER JOIN (SELECT sa.standplaats_id
                                 , sa.adres_id
                                 , sp.standplaatsnummer
                            FROM   G0363_Basis.standplaats_adres sa
                            JOIN   G0363_Basis.standplaats       sp ON  sa.standplaats_id = sp.standplaats_id AND
                                                                  sa.standplaatsvolgnummer = sp.standplaatsvolgnummer
                            WHERE  sp.indauthentiek = 'J'
                            GROUP BY sa.standplaats_id
                                   , sa.adres_id
                                   , sp.standplaatsnummer) q5 ON a.adres_id = q5.adres_id
