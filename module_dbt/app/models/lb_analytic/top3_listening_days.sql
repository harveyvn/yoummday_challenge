select A.user_id, u.user_name, A."date", A.number_of_listens
from
    (
        select
            l.user_id, l."date", count(l.id) as number_of_listens,
            row_number() over(
                partition by l.user_id
                order by count(l.id) desc
            ) as idx_rank
        from {{ source('public', 'listens') }} l
        group by  l.user_id , l."date"
        order by l.user_id , number_of_listens desc, date desc
    ) as A
join users u on A.user_id = u.id
where A.idx_rank < 4
