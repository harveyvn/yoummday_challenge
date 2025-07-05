with active_users_per_day as (
	select A."date", count(distinct l.user_id)
	from {{ source ('public', 'listens') }} l
	join (
	    select distinct l."date" from {{ source('public', 'listens') }} l
     ) as A
	on l."date" >= A."date" - interval '6 days' and l."date" <= A."date"
	group by A."date"
),
total_users as (
	select count(*) from {{ source('public', 'users')}}
)

select
	active_users_per_day.date,
	active_users_per_day.count as number_active_users,
	round((active_users_per_day.count * 1.0 / total_users.count * 1.0) * 100.0, 2) as percentage_active_users
from total_users, active_users_per_day
