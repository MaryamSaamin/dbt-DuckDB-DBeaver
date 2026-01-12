{{ config(
    materialized='incremental',
    unique_key='user_id'
) }}

with first_last_song as (
    select
        user_id,
        first_value(song_name) over (partition by user_id order by event_happened_at) as first_song_name,
        min(event_happened_at) over (partition by user_id) as first_played_at,
        first_value(song_name) over (partition by user_id order by event_happened_at desc) as last_played_song_name,
        max(event_happened_at) over (partition by user_id) as last_played_at
    from {{ ref('stg_song_played') }}
),

guitar_challenges as (
    select
        user_id,
        count(distinct event_id) as challenge_times
    from {{ ref('stg_challenge_opened') }}
    where instrument = 'guitar'
    group by user_id
),

song_after_challenge as (
    select 
        sp.user_id,
        sp.song_name as first_song,
        ch.challenge_name as first_challenge,
        sp.event_happened_at as first_song_played_after_challenge_at
    from (
        select *,
            row_number() over (partition by user_id order by event_happened_at - ch.event_happened_at) as rn
        from {{ ref('stg_song_played') }} sp
        join {{ ref('stg_challenge_opened') }} ch
            on sp.user_id = ch.user_id
        where sp.event_happened_at > ch.event_happened_at
          and sp.event_happened_at - ch.event_happened_at <= 2000
    ) sp
    where rn = 1
)

select
    l.user_id,
    l.country,
    min(l.login_at) as first_login,
    fl.first_song_name,
    fl.first_played_at,
    fl.last_played_song_name,
    fl.last_played_at,
    gc.challenge_times,
    sac.first_song as first_challenge_song,
    sac.first_challenge,
    sac.first_song_played_after_challenge_at
from {{ ref('stg_login') }} l
left join (
    select user_id,
           max(first_song_name) as first_song_name,
           max(first_played_at) as first_played_at,
           max(last_played_song_name) as last_played_song_name,
           max(last_played_at) as last_played_at
    from first_last_song
    group by user_id
) fl on l.user_id = fl.user_id
left join guitar_challenges gc on l.user_id = gc.user_id
left join song_after_challenge sac on l.user_id = sac.user_id

{% if is_incremental() %}
  -- Only add new users in incremental runs
  where l.login_at > (select max(first_login) from {{ this }})
{% endif %}
