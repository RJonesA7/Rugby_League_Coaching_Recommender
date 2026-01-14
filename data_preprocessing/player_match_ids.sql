UPDATE player_stats ps
SET match_id = m.match_id
FROM matches m
WHERE ps.season = m.season
  AND ps.round = m.round
  AND (
        (ps.home = m.home AND ps.away = m.away)
     OR (ps.home = m.away AND ps.away = m.home)
  );
