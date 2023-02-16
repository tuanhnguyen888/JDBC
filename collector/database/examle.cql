-- Create a keyspace
CREATE KEYSPACE IF NOT EXISTS collector WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };

-- Create a table
CREATE TABLE IF NOT EXISTS collector.logs (
id text PRIMARY KEY,
level ASCII,
last_update_timestamp timestamp
);

-- Insert some data
INSERT INTO collector.logs
(id,level, last_update_timestamp)
VALUES ('9876', 'Imformation', toTimeStamp(now()));
INSERT INTO collector.logs
(id,level, last_update_timestamp)
VALUES ('1234', 'Imformation', toTimeStamp(now()));


set https_proxy=http://192.168.5.8:3128
set http_proxy=http://192.168.5.8:3128
go env -w GOPROXY=https://proxy.golang.org,direct

