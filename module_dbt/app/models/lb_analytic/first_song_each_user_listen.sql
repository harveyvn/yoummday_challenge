select l.user_id, u.user_name, l.track_id, t.track_name, t.artist_name, t.release_name, l.listened_at
from {{ source('public', 'listens') }} l
join (
	select l.user_id, min(l.listened_at)
	from {{ source('public', 'listens') }} l
	group by l.user_id
) as A on A.user_id = l.user_id and A.min = l.listened_at
join {{ source('public', 'users') }} u on l.user_id = u.id
join {{ source('public', 'tracks') }} t on l.track_id = t.id
order by l.listened_at
