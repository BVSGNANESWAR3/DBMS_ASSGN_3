import psycopg2
conn = psycopg2.connect(host="localhost", dbname="assgn2", user="postgres", password="myPassword")
cur=conn.cursor()

cur.execute("SET STANDARD_CONFORMING_STRINGS TO OFF;")
cur.execute("SHOW STANDARD_CONFORMING_STRINGS;")

cur.execute("""
  CREATE TABLE papers(
   id integer PRIMARY KEY,
   title text,
   abstract text
)
""")

cur.execute("""
    CREATE TABLE mainauthor(
    id integer,
    mainauthor text,
    CONSTRAINT fk_mainauthor
    FOREIGN KEY(id) 
    REFERENCES papers(id)
)
""")

cur.execute("""
    CREATE TABLE coauthors(
    id integer,
    coauthor text,
    orderlevel integer,
    CONSTRAINT fk_coauthors
    FOREIGN KEY(id) 
    REFERENCES papers(id)
)
""")

cur.execute("""
    CREATE TABLE venue(
    id integer,
    venue text,
    CONSTRAINT fk_venue
    FOREIGN KEY(id) 
    REFERENCES papers(id)
)
""")

cur.execute("""
    CREATE TABLE year(
    id integer,
    year integer,
    CONSTRAINT fk_year
    FOREIGN KEY(id) 
    REFERENCES papers(id)
)
""")

cur.execute("""
    CREATE TABLE referenceslist(
    id integer,
    referenceid integer,
    CONSTRAINT fk_referenceslist
    FOREIGN KEY(id) 
    REFERENCES papers(id)
)
""")

with open('papers.tsv','r') as f:
	next(f)
	cur.copy_from(f, 'papers', sep='\t')
with open('mainauthor.tsv','r') as f:
	next(f)	
	cur.copy_from(f, 'mainauthor', sep='\t')
with open('coauthor.tsv','r') as f:
	next(f)
	cur.copy_from(f, 'coauthors', sep='\t')
with open('venue.tsv','r') as f:
	next(f)
	cur.copy_from(f, 'venue', sep='\t')
with open('year.tsv','r') as f:
	next(f)
	cur.copy_from(f, 'year', sep='\t')
with open('referenceslist.tsv','r') as f:
	next(f)
	cur.copy_from(f, 'referenceslist', sep='\t')
conn.commit()
cur.close()
conn.close()








