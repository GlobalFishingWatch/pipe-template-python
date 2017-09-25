SELECT
  messages.lat        AS lat,
  messages.lon        AS lon,
  messages.score      AS score,
  messages.timestamp  AS timestamp,
  vessels.seriesgroup AS id
FROM
  [world-fishing-827:pipeline_tileset_p_p516_daily.516_resample_v2_2017_09_08_messages] messages
  INNER JOIN [world-fishing-827:pipeline_tileset_p_p516_daily.516_resample_v2_2017_09_08_vessels] vessels
     ON messages.seg_id == vessels.seg_id
WHERE
  messages.score IS NOT NULL
LIMIT
  1000
