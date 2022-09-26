select b.id 
from s_buildings_copy as b 
    left join s_pb as p on b.id = p.s_building_id 
where p.s_building_id is NULL order by 1;