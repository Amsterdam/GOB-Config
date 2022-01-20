-- Due to changes in the coding system for wijken, add a case for before and after the change on 24-03-2022
-- Stadsdeel is already contained in the wijk code
-- before: code = stadsdeel + wijk + buurt, after: code = wijk + buurt
SELECT s1.naam                                                             AS naam
,      CASE
       WHEN nvl(q2.einddatum, q3.ingsdatum) <= to_date('2022-03-24', 'YYYY-MM-DD')
           THEN s1.sdlcode || s1.wijkcode
       ELSE s1.wijkcode
       END                                                                 AS code
,      q2.volgnummer                                                       AS volgnummer
,      to_char(q2.ingsdatum, 'YYYY-MM-DD')                                 AS begin_geldigheid
,      to_char(nvl(q2.einddatum, q3.ingsdatum), 'YYYY-MM-DD')              AS eind_geldigheid
,      to_char(s1.docdatum, 'YYYY-MM-DD')                                  AS documentdatum
,      s1.docnr                                                            AS documentnummer
,      s1.sdlcode                                                          AS ligt_in_stadsdeel
,      to_char(t1.inwin, 'YYYY-MM-DD HH24:MI:SS')                          AS registratiedatum
,      t1.guid                                                             AS source_id
,      to_char(nvl(q2.einddatum, q3.ingsdatum), 'YYYY-MM-DD HH24:MI:SS')   AS expirationdate
,      sdo_util.to_wktgeometry(t1.geometrie)                               AS geometrie
FROM   gebieden.dgdtw_topografie t1
JOIN   gebieden.dgdtw_table_6025 s1 ON t1.id = s1.dgdtw_primary_key
-- volgnummer afleiden
JOIN  (SELECT t2.id
       ,      t2.guid
       ,      s2.ingsdatum
       ,      s2.einddatum
       ,      dense_rank() OVER (partition BY t2.guid ORDER BY t2.inwin) AS volgnummer
       FROM   gebieden.dgdtw_topografie t2
       JOIN   gebieden.dgdtw_table_6025 s2 ON t2.id = s2.dgdtw_primary_key) q2 ON t1.id = q2.id
       -- einddatum toestand selecteren (kan leeg zijn) van volgende cyclus
       LEFT OUTER JOIN  (SELECT t3.guid
                         ,      t3.inwin
                         ,      s3.ingsdatum
                         FROM   gebieden.dgdtw_topografie t3
                         JOIN   gebieden.dgdtw_table_6025 s3 ON t3.id = s3.dgdtw_primary_key) q3 ON t1.guid = q3.guid AND t1.verval = q3.inwin
WHERE  t1.objectcode = 6025 -- wijk
ORDER BY code, volgnummer
