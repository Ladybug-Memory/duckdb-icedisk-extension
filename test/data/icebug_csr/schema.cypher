CREATE NODE TABLE user(id INT64, name STRING, PRIMARY KEY(id)) WITH (storage = './icebug_csr', format = 'icebug-disk');
CREATE NODE TABLE city(id INT64, name STRING, PRIMARY KEY(id)) WITH (storage = './icebug_csr', format = 'icebug-disk');
CREATE REL TABLE follows(FROM user TO user, since INT32) WITH (storage = './icebug_csr', format = 'icebug-disk');
CREATE REL TABLE livesin(FROM user TO city) WITH (storage = './icebug_csr', format = 'icebug-disk');
