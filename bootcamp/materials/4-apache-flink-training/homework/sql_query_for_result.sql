-- What is the average number of web events of a session from a user on Tech Creator?
-- SQL Query
SELECT host,
    	ROUND(AVG(event_count), 1) as avg_events_per_session
	
FROM sessionized_events
WHERE host = 'www.techcreator.io'
GROUP BY host

-- Answer: 2.4

---------------------------------------------------------------------

-- Compare results between different hosts (zachwilson.techcreator.io, zachwilson.tech, lulu.techcreator.io)
-- SQL Query
SELECT host,
    	ROUND(AVG(event_count), 1) as avg_events_per_session
	
FROM sessionized_events
WHERE host IN ('zachwilson.techcreator.io', 'zachwilson.tech', 'lulu.techcreator.io', 'www.techcreator.io')
GROUP BY host
ORDER BY avg_events_per_session DESC;

-- Answer: There were no events for zachwilson.tech, so the results are as follows:
-- ------------------------------------------------------
-- |"host"	                   |"avg_events_per_session"|
-- ------------------------------------------------------
-- |"lulu.techcreator.io"      | 4.5                    |
-- |"zachwilson.techcreator.io"| 3.8                    |
-- |"www.techcreator.io"       | 2.4                    |
-- ------------------------------------------------------