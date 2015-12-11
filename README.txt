# Vytvoření KEYSPACE:
CREATE KEYSPACE dostam12_ks WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '3'
};

# Vytvoření TABLE
CREATE TABLE logs (
id text primary key,
imp_id int,
company text,
website text,
app text,
price double,
cur text,
win int,
bidfloor double,
cpt double,
auction_price double,
datetime timestamp,
);

# Import dat
cqlsh:dostam12_ks> source '/home/user/Downloads/insert.cql'

# Solr configurace a indexace
curl --noproxy '*' http://localhost:8983/solr/resource/dostam12_ks.logs/solrconfig.xml --data-binary @solrconfig.xml -H 'Content-type:text/xml; charset=utf-8'
curl --noproxy '*' http://localhost:8983/solr/resource/dostam12_ks.logs/schema.xml --data-binary @schema.xml -H 'Content-type:text/xml; charset=utf-8'
curl --noproxy '*' "http://localhost:8983/solr/admin/cores?action=CREATE&name=dostam12_ks.logs"