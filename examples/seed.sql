CREATE TABLE demo (id BIGINT, name STRING, amount DECIMAL(18,2), active BOOLEAN);
INSERT INTO demo VALUES (1,'Alice',12.34,true),(2,'AbC 中文',56.78,false),(3,NULL,NULL,NULL);
CREATE TABLE events (id BIGINT, value STRING) PARTITIONED BY (ds STRING);
INSERT INTO events PARTITION(ds='20260914') VALUES (1,'first'),(2,'second');
INSERT INTO events PARTITION(ds='20260915') VALUES (3,'next day');
