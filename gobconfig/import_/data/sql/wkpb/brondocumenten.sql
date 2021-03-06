SELECT b.subject2                                       AS documentnummer
,      to_char(b.item_created, 'YYYY-MM-DD HH24:MI:SS') AS registratiedatum
,      b.item_key                                       AS source_id
FROM   decos.item d
JOIN   decos.item b ON d.item_key = b.it_parent_key
JOIN   (SELECT p.subject2
        ,      MAX(p.item_created) AS item_created
        FROM decos.item p
        GROUP BY p.subject2) q ON  q.subject2 = b.subject2
                               AND b.item_created = q.item_created
WHERE  b.subject1 like '%IDOCWKPB%'
AND    d.text4 = 'WK'
AND    upper(substr(b.subject2,16,2))  = 'WK'
AND    d.item_deleted is null
AND    b.item_deleted is NULL