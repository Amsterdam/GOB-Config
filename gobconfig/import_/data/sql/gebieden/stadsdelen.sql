SELECT s1.naam                                                             AS naam
,      s1.sdlcode                                                          AS code
,      dense_rank() OVER (partition BY q2.guid ORDER BY q3.inwin)          AS volgnummer
,      to_char(q2.ingsdatum, 'YYYY-MM-DD')                                 AS begin_geldigheid
,      to_char(nvl(q2.einddatum, q3.ingsdatum), 'YYYY-MM-DD')              AS eind_geldigheid
,      to_char(s1.docdatum, 'YYYY-MM-DD')                                  AS documentdatum
,      s1.docnr                                                            AS documentnummer
,      to_char(t1.inwin, 'YYYY-MM-DD HH24:MI:SS')                          AS registratiedatum
,      t1.guid                                                             AS source_id
,      to_char(nvl(q2.einddatum, q3.ingsdatum), 'YYYY-MM-DD HH24:MI:SS')   AS expirationdate
,      sdo_util.to_wktgeometry(t1.geometrie)                               AS geometrie
FROM   gebieden.dgdtw_topografie t1
JOIN   gebieden.dgdtw_table_6027 s1 ON t1.id = s1.dgdtw_primary_key
-- volgnummer afleiden
JOIN  (SELECT t2.id
       ,      s2.ingsdatum
       ,      s2.einddatum
       ,      t2.guid
       FROM   gebieden.dgdtw_topografie t2
       JOIN   gebieden.dgdtw_table_6027 s2 ON t2.id = s2.dgdtw_primary_key) q2 ON t1.id = q2.id
       -- einddatum toestand selecteren (kan leeg zijn) van volgende cyclus
       LEFT OUTER JOIN  (SELECT t3.guid
                         ,      t3.inwin
                         ,      s3.ingsdatum
                         FROM   gebieden.dgdtw_topografie t3
                         JOIN   gebieden.dgdtw_table_6027 s3 ON t3.id = s3.dgdtw_primary_key) q3 ON t1.guid = q3.guid AND t1.verval = q3.inwin
WHERE  t1.objectcode = 6027 -- stadsdeel
        AND (q2.ingsdatum <> nvl(q2.einddatum, q3.ingsdatum) OR nvl(q2.einddatum, q3.ingsdatum) IS NULL) -- exclude intervals with length 0
ORDER BY s1.sdlcode, volgnummer
