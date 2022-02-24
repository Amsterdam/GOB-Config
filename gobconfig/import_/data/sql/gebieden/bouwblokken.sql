WITH src AS (
    SELECT s.code                                                             AS code
         , NULL                                                               AS identificatie
         , dense_rank() OVER (partition BY s.code ORDER BY t.inwin, t.verval) AS volgnummer
         , to_char(t.inwin, 'YYYY-MM-DD HH24:MI:SS')                          AS begin_geldigheid
         , to_char(t.verval, 'YYYY-MM-DD HH24:MI:SS')                         AS eind_geldigheid
         , to_char(t.inwin, 'YYYY-MM-DD HH24:MI:SS')                          AS registratiedatum
         -- source_id should be unique -> combine guid and code
         -- some records have the same guid, but different code
         -- code is used as identification in gob
         , t.guid || '.' || s.code                                            AS source_id
         , to_char(t.verval, 'YYYY-MM-DD HH24:MI:SS')                         AS expirationdate
         , sdo_util.to_wktgeometry(SDO_UTIL.SIMPLIFY(t.geometrie, 0.05))      AS geometrie
         , NULL                                                               AS ligt_in_buurt
    FROM gebieden.dgdtw_topografie t
             JOIN gebieden.dgdtw_table_6023 s ON t.id = s.dgdtw_primary_key
    WHERE t.objectcode = 6023
)
select s.*
from src s
    left join (
-- Filter nonvalid records:
--  * which have a higher sequencenumber and an endvalidity
--  * than a record with no end validity (=actual)
-- those records can be safely discarded,
-- because these records are not closed properly wrt the timeline of that entity (opened and closed after actual)
    select t.code, t.volgnummer
    from src s
             join src t
                  on s.code = t.code and
                     (s.volgnummer < t.volgnummer and
                      s.eind_geldigheid is null and  --open
                      t.eind_geldigheid is not null) -- closed
    ) nv
        on s.code = nv.code and s.volgnummer = nv.volgnummer
where nv.code is null
