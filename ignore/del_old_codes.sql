UPDATE results
SET code_3 = 'none'
WHERE 
    (event LIKE '%band%' AND code_3 IN (SELECT code FROM pml WHERE event_name NOT LIKE '%band%'))
    OR (event LIKE '%chorus%' AND code_3 IN (SELECT code FROM pml WHERE event_name NOT LIKE '%chorus%' AND event_name NOT LIKE '%madrigal%'))
    OR (event LIKE '%orchestra%' AND code_3 IN (SELECT code FROM pml WHERE event_name NOT LIKE '%orchestra%'));