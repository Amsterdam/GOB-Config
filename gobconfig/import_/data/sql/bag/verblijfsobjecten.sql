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
                       WHERE  objecttype = 102 ORDER BY object_identificatie),
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
                             FROM   basis.verblijfseenheid
                             WHERE  indauthentiek = 'J'),
    -- SubQuery factoring for begin and eindgeldigheid
    -- begindatum gebruiken als einddatum volgende cyclus
    begin_cyclus AS (SELECT verblijfseenheidnummer
	                      , verblijfseenheidvolgnummer
	                      , 1 + dense_rank() OVER (partition BY verblijfseenheidnummer ORDER BY verblijfseenheidvolgnummer) AS rang
	                 FROM   authentieke_objecten),
    eind_cyclus AS (SELECT verblijfseenheidnummer
	                     , verblijfseenheidvolgnummer
	                     , datumopvoer
                         , nvl(trunc(datumopvoer), max_date()) as eind_cyclus
	                     , dense_rank() OVER (partition BY verblijfseenheidnummer ORDER BY verblijfseenheidvolgnummer) AS rang
	                FROM   authentieke_objecten),
    -- SubQuery factoring for shared datasets
    adressen AS (SELECT   adres_id
                        , adresnummer
                 FROM     basis.adres
                 WHERE    indauthentiek = 'J'
                 GROUP BY adres_id, adresnummer)
SELECT v.verblijfseenheidnummer                                                               AS identificatie
     , v.verblijfseenheidvolgnummer                                                           AS volgnummer
     , s.status                                                                               AS status_code
     , s.omschrijving                                                                         AS status_omschrijving
     , to_char(v.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')                                        AS begin_geldigheid
     , to_char(q2.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')                                       AS eind_geldigheid
     , (SELECT CASE
               WHEN listagg(inonderzoek, ';') WITHIN GROUP (ORDER BY object_identificatie) LIKE '%J%'
               THEN 'J'
               ELSE 'N'
               END
        FROM  in_onderzoeken_eod io
        WHERE io.object_identificatie = v.verblijfseenheidnummer
          AND cyclus_in_onderzoek(trunc(v.datumopvoer), q2.eind_cyclus,
                                  io.begin_onderzoek, io.eind_onderzoek) = 1
       )                                                                                      AS aanduiding_in_onderzoek
     , (SELECT listagg(identificatie, ';')
        FROM   in_onderzoeken io
        WHERE  object_identificatie = v.verblijfseenheidnummer
          AND  cyclus_in_onderzoek(trunc(v.datumopvoer), q2.eind_cyclus,
                                   io.begin_onderzoek, io.eind_onderzoek) = 1
       )                                                                                      AS heeft_onderzoeken
     , v.indgeconstateerd                                                                     AS geconstateerd
     , to_char(v.dd_document, 'YYYY-MM-DD')                                                   AS documentdatum
     , v.documentnummer                                                                       AS documentnummer
     , TRIM(nvl(regexp_substr(v.documentnummer, '^(.*?)_', 1, 1, NULL, 1), v.documentnummer)) AS dossier
     , to_char(v.creation, 'YYYY-MM-DD HH24:MI:SS')                                           AS registratiedatum
     , v.verblijfseenheid_id                                                                  AS source_id
     , NVL2(q2.datumopvoer,
            CASE
            WHEN q2.datumopvoer < sysdate
            THEN to_char(q2.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')
            ELSE to_char(v.modification, 'YYYY-MM-DD HH24:MI:SS')
            END
    , CASE
      WHEN s.status NOT IN (1, 3, 4, 6, 7)
      THEN CASE
           WHEN q2.datumopvoer < sysdate
           THEN to_char(v.datumopvoer, 'YYYY-MM-DD HH24:MI:SS')
           ELSE to_char(v.creation, 'YYYY-MM-DD HH24:MI:SS')
           END
      ELSE NULL
      END)                                                                                    AS expirationdate
     , v.bagproces                                                                            AS bagproces_code
     , m.omschrijving                                                                         AS bagproces_omschrijving
     , v.vloeroppervlakte                                                                     AS oppervlakte
     , ROUND(v.aantal_bouwlagen)                                                              AS aantal_bouwlagen
     , ROUND(v.hoogste_bouwlaag)                                                              AS hoogste_bouwlaag
     , ROUND(v.laagste_bouwlaag)                                                              AS laagste_bouwlaag
     , v.aantal_verhuurbare_eenheden                                                          AS aantal_verhuurbare_eenheden
     , ROUND(v.toegang_bouwlaag)                                                              AS verdieping_toegang
     , v.redenopvoer                                                                          AS redenopvoer_code
     , ov.omschrijving                                                                        AS redenopvoer_omschrijving
     , v.redenafvoer                                                                          AS redenafvoer_code
     , av.omschrijving                                                                        AS redenafvoer_omschrijving
     , v.woonvertrekken                                                                       AS aantal_kamers
     , v.eigenaar                                                                             AS eigendomsverhouding_code
     , e.omschrijving                                                                         AS eigendomsverhouding_omschrijv
     , v.woonverblijfsoort                                                                    AS feitelijk_gebruik_code
     , f.omschrijving                                                                         AS feitelijk_gebruik_omschrijving
     , v.financieringsvorm                                                                    AS fng_code
     , v.cbsnummer                                                                            AS cbs_nummer
     , v.woningvoorraad                                                                       AS woningvoorraad
     , q3.gebruiksdoel                                                                        AS gebruiksdoel
     , q6.gebruiksdoel_woonfunctie                                                            AS gebruiksdoel_woonfunctie
     , q7.gebruiksdoel_gezondheidszorg                                                        AS gebruiksdoel_gezondheidszorg
     , q5.toegang                                                                             AS toegang
     , q1.adresnummer                                                                         AS nummeraanduidingid_hoofd
     , q2.adresnummer                                                                         AS nummeraanduidingid_neven
     , q4.pandidentificatie                                                                   AS pandidentificatie
     , sdo_util.to_wktgeometry(v.geometrie)                                                   AS geometrie
FROM authentieke_objecten v
    -- begindatum gebruiken als einddatum volgende cyclus
	    JOIN begin_cyclus q1 ON v.verblijfseenheidnummer = q1.verblijfseenheidnummer AND
	                            v.verblijfseenheidvolgnummer = q1.verblijfseenheidvolgnummer
	    LEFT OUTER JOIN eind_cyclus q2 ON  q1.verblijfseenheidnummer = q2.verblijfseenheidnummer AND
	                                       q1.rang = q2.rang
    -- selecteren status
         LEFT OUTER JOIN basis.verblijfsobjectstatus s ON v.status_id = s.status
    -- selecteren bagproces / mutatiereden
         LEFT OUTER JOIN basis.mutatiereden m ON v.bagproces = m.id
    -- selecteren type woonobject
         LEFT OUTER JOIN basis.woningtype wt ON v.woningtype = wt.woningtype
    -- selecteren eigendomsverhouding
         LEFT OUTER JOIN basis.eigenaar e ON v.eigenaar = e.id
    -- selecteren feitelijk_gebruik
         LEFT OUTER JOIN basis.woonverblijfsoort f ON v.woonverblijfsoort = f.woonverblijfsoort
    -- selecteren redenopvoer
         LEFT OUTER JOIN basis.opvoerreden_verblijfseenheid ov ON v.redenopvoer = ov.opvoerreden
    -- selecteren redenafvoer
         LEFT OUTER JOIN basis.afvoerreden_verblijfseenheid av ON v.redenafvoer = av.afvoerreden
    -- selecteren hoofdadres(sen)
         LEFT OUTER JOIN (SELECT va.verblijfseenheid_id
                               , va.verblijfseenheidvolgnummer
                               , a.adresnummer
                          FROM basis.verblijfseenheid_adres va
                          JOIN adressen a ON a.adres_id = va.adres_id
                          WHERE va.indhoofdadres = 'J') q1 ON v.verblijfseenheid_id = q1.verblijfseenheid_id AND
                                                              v.verblijfseenheidvolgnummer = q1.verblijfseenheidvolgnummer
    -- selecteren nevenadres(sen)
         LEFT OUTER JOIN (SELECT va.verblijfseenheid_id
                               , va.verblijfseenheidvolgnummer
                               , listagg(a.adresnummer, ';')
                                 WITHIN GROUP (ORDER BY va.verblijfseenheid_id, va.verblijfseenheidvolgnummer) AS adresnummer
                          FROM basis.verblijfseenheid_adres va
                          JOIN adressen a ON a.adres_id = va.adres_id
                          WHERE va.indhoofdadres = 'N'
                          GROUP BY va.verblijfseenheid_id, va.verblijfseenheidvolgnummer) q2 ON v.verblijfseenheid_id = q2.verblijfseenheid_id AND
                                                                                                v.verblijfseenheidvolgnummer = q2.verblijfseenheidvolgnummer
    -- selecteren pand(en)
         LEFT OUTER JOIN (SELECT q.verblijfseenheid_id
                               , q.verblijfseenheidvolgnummer
                               , listagg(q.gebouwnummer, ';')
                                 WITHIN GROUP (ORDER BY q.verblijfseenheid_id, q.verblijfseenheidvolgnummer) AS pandidentificatie
                          FROM (SELECT vg.verblijfseenheid_id
                                     , vg.verblijfseenheidvolgnummer
                                     , g.gebouw_id
                                     , g.gebouwnummer
                                     , MAX(g.gebouwvolgnummer) AS gebouwvolgnummer
                                FROM basis.verblijfseenheid_gebouw vg
                                         JOIN basis.gebouw g ON vg.gebouw_id = g.gebouw_id
                                    AND vg.gebouwvolgnummer = g.gebouwvolgnummer
                                WHERE g.indauthentiek = 'J'
                                GROUP BY vg.verblijfseenheid_id
                                       , vg.verblijfseenheidvolgnummer
                                       , g.gebouw_id
                                       , g.gebouwnummer) q
                          GROUP BY q.verblijfseenheid_id, q.verblijfseenheidvolgnummer) q4 ON v.verblijfseenheid_id = q4.verblijfseenheid_id AND
                                                                                              v.verblijfseenheidvolgnummer = q4.verblijfseenheidvolgnummer
    -- select gebruiksdoel(en)
         LEFT OUTER JOIN (SELECT vg.verblijfsobject_id
                               , vg.verblijfsobjectvolgnummer
                               , listagg(g.gebruiksdoel_id || '|' || g.omschrijving, ';')
                                 WITHIN GROUP (ORDER BY vg.verblijfsobject_id,vg.verblijfsobjectvolgnummer) AS gebruiksdoel
                          FROM basis.verblijfsobject_gebruiksdoel vg
                                   JOIN basis.gebruiksdoel_vbo g ON vg.gebruiksdoel_id = g.gebruiksdoel_id
                                   JOIN basis.verblijfseenheid v ON vg.verblijfsobject_id = v.verblijfseenheid_id
                              AND vg.verblijfsobjectvolgnummer = v.verblijfseenheidvolgnummer
                          WHERE v.indauthentiek = 'J'
                          GROUP BY vg.verblijfsobject_id, vg.verblijfsobjectvolgnummer) q3 ON v.verblijfseenheid_id = q3.verblijfsobject_id AND
                                                                                              v.verblijfseenheidvolgnummer = q3.verblijfsobjectvolgnummer
    -- select toegang
         LEFT OUTER JOIN (SELECT vt.id                                       AS verblijfsobject_id
                               , vt.volgnummer                               AS verblijfsobjectvolgnummer
                               , listagg(t.toegankelijkheid || '|' || TRIM(regexp_substr(omschrijving, '[^(]*')), ';')
                                 WITHIN GROUP (ORDER BY vt.id,vt.volgnummer) AS toegang
                          FROM basis.verblijfseenheid_toegang vt
                               JOIN basis.toegankelijkheid t ON vt.id_toegankelijkheid = t.toegankelijkheid
                               JOIN basis.verblijfseenheid v ON vt.id = v.verblijfseenheid_id AND
                                                                vt.volgnummer = v.verblijfseenheidvolgnummer
                          WHERE v.indauthentiek = 'J'
                          GROUP BY vt.id, vt.volgnummer) q5 ON v.verblijfseenheid_id = q5.verblijfsobject_id AND
                                                               v.verblijfseenheidvolgnummer = q5.verblijfsobjectvolgnummer
    -- select gebruiksdoel_woonfunctie
         LEFT OUTER JOIN (SELECT vg.verblijfsobject_id
                               , vg.verblijfsobjectvolgnummer
                               , CASE g.gebruiksdoel_id
                                 WHEN 1
                                 THEN CASE
                                      WHEN v.basiseenheidtype IN ('1010', '2075')
                                      THEN NULL
                                      ELSE v.basiseenheidtype || '|' || UPPER(substr(b.omschrijving, 6, 1)) ||
                                                          substr(b.omschrijving, 7, LENGTH(b.omschrijving) - 5)
                                      END
                                 END AS gebruiksdoel_woonfunctie
                          FROM basis.verblijfsobject_gebruiksdoel vg
                               JOIN basis.gebruiksdoel_vbo g ON vg.gebruiksdoel_id = g.gebruiksdoel_id
                               JOIN basis.verblijfseenheid v ON vg.verblijfsobject_id = v.verblijfseenheid_id AND
                                                                vg.verblijfsobjectvolgnummer = v.verblijfseenheidvolgnummer
                               JOIN basis.basiseenheidtype b ON v.basiseenheidtype = b.code
                          WHERE v.indauthentiek = 'J' AND g.gebruiksdoel_id = 1) q6 ON v.verblijfseenheid_id = q6.verblijfsobject_id AND
                                                                                       v.verblijfseenheidvolgnummer = q6.verblijfsobjectvolgnummer
    -- select gebruiksdoel_gezondheidszorgfunctie
         LEFT OUTER JOIN (SELECT vg.verblijfsobject_id
                               , vg.verblijfsobjectvolgnummer
                               , v.basiseenheidtype || '|' || UPPER(substr(b.omschrijving, 6, 1)) ||
                                 substr(b.omschrijving, 7, LENGTH(b.omschrijving) - 5) AS gebruiksdoel_gezondheidszorg
                          FROM (SELECT x.verblijfsobject_id
                                     , x.verblijfsobjectvolgnummer
                                     , MIN(x.gebruiksdoel_id) AS min_gebruiksdoel
                                FROM basis.verblijfsobject_gebruiksdoel x
                                GROUP BY x.verblijfsobject_id
                                       , x.verblijfsobjectvolgnummer) q1
                                   JOIN basis.verblijfsobject_gebruiksdoel vg
                                        ON q1.verblijfsobject_id = vg.verblijfsobject_id
                                            AND q1.verblijfsobjectvolgnummer = vg.verblijfsobjectvolgnummer
                                   JOIN basis.verblijfseenheid v ON vg.verblijfsobject_id = v.verblijfseenheid_id
                              AND vg.verblijfsobjectvolgnummer = v.verblijfseenheidvolgnummer
                                   JOIN basis.basiseenheidtype b ON v.basiseenheidtype = b.code
                          WHERE v.indauthentiek = 'J' AND
                                vg.gebruiksdoel_id = 4 AND
                                q1.min_gebruiksdoel != 1) q7 ON v.verblijfseenheid_id = q7.verblijfsobject_id AND
                                                                v.verblijfseenheidvolgnummer = q7.verblijfsobjectvolgnummer
