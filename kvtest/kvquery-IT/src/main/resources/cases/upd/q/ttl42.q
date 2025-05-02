UPDATE NoTTL $j
SET TTL -1 DAYS
WHERE id = 40
RETURNING remaining_days($j) AS Expires
