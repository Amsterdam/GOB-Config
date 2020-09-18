SELECT TRIM(NVL(regexp_substr(substr(b.subject2, 1, 21), '^(.*?)_', 1, 1, NULL, 1), substr(b.subject2, 1, 21))) AS dossier
,      listagg(substr(b.subject2, 1, 21), ';') WITHIN GROUP (ORDER BY 1)                                        AS heeft_wkpb_brondocument 
FROM   decos.item d
JOIN   decos.item b ON d.item_key = b.it_parent_key
WHERE  b.subject1 like '%IDOCWKPB%'
AND    d.text4 = 'WK'
AND    upper(substr(b.subject2,16,2))  = 'WK'
AND    d.item_deleted is null
AND    b.item_deleted is null
GROUP BY TRIM(NVL(regexp_substr(substr(b.subject2, 1, 21), '^(.*?)_', 1, 1, NULL, 1), substr(b.subject2, 1, 21)))