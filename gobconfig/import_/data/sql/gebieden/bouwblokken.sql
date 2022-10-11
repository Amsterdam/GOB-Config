SELECT s.code                                                        AS code
,      NULL                                                          AS identificatie
,      dense_rank() OVER (partition BY s.code ORDER BY t.inwin)      AS volgnummer
,      to_char(t.inwin, 'YYYY-MM-DD')                                AS begin_geldigheid
,      to_char(t.verval, 'YYYY-MM-DD')                               AS eind_geldigheid
,      to_char(t.inwin, 'YYYY-MM-DD HH24:MI:SS')                     AS registratiedatum
,      t.guid                                                        AS source_id
,      to_char(t.verval, 'YYYY-MM-DD HH24:MI:SS')                    AS expirationdate
,      sdo_util.to_wktgeometry(SDO_UTIL.SIMPLIFY(t.geometrie, 0.05)) AS geometrie
,      NULL                                                          AS ligt_in_buurt
FROM   gebieden.dgdtw_topografie t
JOIN   gebieden.dgdtw_table_6023 s ON t.id = s.dgdtw_primary_key
WHERE  t.objectcode = 6023
  AND (t.INWIN <> t.VERVAL OR t.VERVAL IS NULL)  -- exclude intervals with length 0
ORDER BY s.code, volgnummer
