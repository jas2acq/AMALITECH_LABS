-- Create the table to store heartbeat data
CREATE TABLE IF NOT EXISTS heartbeat_data (

    customer_id VARCHAR(255) PRIMARY KEY,

    -- Timestamp of the heartbeat reading (using TIMESTAMP WITH TIME ZONE for accuracy)
    -- The ISO format from Python is compatible with this type.
    time_stamp TIMESTAMP WITH TIME ZONE NOT NULL,

    -- Heart rate reading
    heart_rate INTEGER NOT NULL
);

-- Create an index on the timestamp column for efficient time-based queries
CREATE INDEX IF NOT EXISTS idx_heartbeat_timestamp
ON heartbeat_data (time_stamp);

-- Create an index on the heart_rate column for efficient filtering and grouping by heart rate.
-- This is useful for dashboard features like Heart Rate Distribution and Individual Event Lookup by heart_rate.
CREATE INDEX IF NOT EXISTS idx_heartbeat_heart_rate
ON heartbeat_data (heart_rate);


