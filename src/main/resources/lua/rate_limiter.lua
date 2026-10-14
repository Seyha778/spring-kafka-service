-- Sliding-window rate limiter.
-- KEYS[1] = rate limit key (e.g. "cashback:rate:<campaignCode>")
-- ARGV[1] = current time in millis
-- ARGV[2] = window size in millis
-- ARGV[3] = max requests allowed per window
-- ARGV[4] = unique member id for this request (e.g. txnId:timestamp)
--
-- Returns 1 if permitted, 0 if rejected.

local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local member = ARGV[4]

-- Drop entries outside the current window
redis.call('ZREMRANGEBYSCORE', key, 0, now - window)

local count = redis.call('ZCARD', key)

if count < limit then
    redis.call('ZADD', key, now, member)
    redis.call('EXPIRE', key, math.ceil(window / 1000) + 1)
    return 1
else
    -- still refresh TTL so the key doesn't linger indefinitely if abandoned
    redis.call('EXPIRE', key, math.ceil(window / 1000) + 1)
    return 0
end
