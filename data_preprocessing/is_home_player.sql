-- 1) add column
ALTER TABLE player_stats
ADD COLUMN IF NOT EXISTS is_home boolean;

-- 2) assign is_home using the fact that the first N per position are home, with N varying by position
WITH ranked AS (
    SELECT
        player_match_id,
        match_id,
        position,
        ROW_NUMBER() OVER (
            PARTITION BY match_id, position
            ORDER BY player_match_id
        ) AS pos_rank
    FROM player_stats
),
expected AS (
    SELECT
        player_match_id,
        match_id,
        position,
        pos_rank,
        CASE
            WHEN position = 'Fullback' THEN 1
            WHEN position = 'Winger' THEN 2
            WHEN position = 'Centre' THEN 2
            WHEN position = 'Halfback' THEN 1
            WHEN position IN ('Five-Eighth','Five Eighth') THEN 1
            WHEN position = 'Hooker' THEN 1
            WHEN position = 'Prop' THEN 2
            WHEN position = '2nd Row' THEN 2
            WHEN position = 'Lock' THEN 1

            -- bench / extras (as per your rule)
            WHEN position = 'Interchange' THEN 4
            WHEN position = 'Reserve' THEN 1
            WHEN position = 'Replacement' THEN 1

            ELSE 0
        END AS per_team_expected
    FROM ranked
)
UPDATE player_stats ps
SET is_home = (e.pos_rank <= e.per_team_expected)
FROM expected e
WHERE ps.player_match_id = e.player_match_id;

-- 1) add column
ALTER TABLE player_stats_z
ADD COLUMN IF NOT EXISTS is_home boolean;

-- 2) assign is_home using "first N per position are home"
WITH ranked AS (
    SELECT
        player_match_id,
        match_id,
        position,
        ROW_NUMBER() OVER (
            PARTITION BY match_id, position
            ORDER BY player_match_id
        ) AS pos_rank
    FROM player_stats_z
),
expected AS (
    SELECT
        player_match_id,
        match_id,
        position,
        pos_rank,
        CASE
            WHEN position = 'Fullback' THEN 1
            WHEN position = 'Winger' THEN 2
            WHEN position = 'Centre' THEN 2
            WHEN position = 'Halfback' THEN 1
            WHEN position IN ('Five-Eighth','Five Eighth') THEN 1
            WHEN position = 'Hooker' THEN 1
            WHEN position = 'Prop' THEN 2
            WHEN position = '2nd Row' THEN 2
            WHEN position = 'Lock' THEN 1

            -- bench / extras (as per your rule)
            WHEN position = 'Interchange' THEN 4
            WHEN position = 'Reserve' THEN 1
            WHEN position = 'Replacement' THEN 1

            ELSE 0
        END AS per_team_expected
    FROM ranked
)
UPDATE player_stats_z ps
SET is_home = (e.pos_rank <= e.per_team_expected)
FROM expected e
WHERE ps.player_match_id = e.player_match_id;

