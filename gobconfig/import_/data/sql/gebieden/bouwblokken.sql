SELECT s.code                                                        AS code
,      NULL                                                          AS identificatie
,      dense_rank() OVER (partition BY q1.guid ORDER BY q1.inwin)    AS volgnummer
,      to_char(t.inwin, 'YYYY-MM-DD')                                AS begin_geldigheid
,      to_char(t.verval, 'YYYY-MM-DD')                               AS eind_geldigheid
,      to_char(t.inwin, 'YYYY-MM-DD HH24:MI:SS')                     AS registratiedatum
,      t.guid                                                        AS source_id
,      to_char(t.verval, 'YYYY-MM-DD HH24:MI:SS')                    AS expirationdate
,      sdo_util.to_wktgeometry(SDO_UTIL.SIMPLIFY(t.geometrie, 0.05)) AS geometrie
,      NULL                                                          AS ligt_in_buurt
FROM   gebieden.dgdtw_topografie t
JOIN   gebieden.dgdtw_table_6023 s ON t.id = s.dgdtw_primary_key
JOIN  (SELECT t1.id
       ,      t1.guid
       ,      t1.inwin
       FROM   gebieden.dgdtw_topografie t1
       JOIN   gebieden.dgdtw_table_6023 s1 ON t1.id = s1.dgdtw_primary_key) q1 ON t.id = q1.id
WHERE  t.objectcode = 6023 --bouwblok
        AND (t.INWIN <> t.VERVAL OR t.VERVAL IS NULL) -- exclude intervals with length 0
ORDER  BY s.code, volgnummer
