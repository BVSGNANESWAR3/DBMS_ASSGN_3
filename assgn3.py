import psycopg2
import pandas as pd
conn = psycopg2.connect(host="localhost", dbname="assgn2", user="postgres", password="myPassword")
cur=conn.cursor()

# cur.execute("""
# 	select * from papers left join venue
# 	on papers.id = venue.id;
# """)

cur.execute("""
    create table authors as select id, mainauthor as authorname from mainauthor;
    insert into authors(id, authorname) select id, coauthor as authorname from coauthors;
""")

cur.execute("""
    create table information as select papers.id, title, venue, authorname, year, abstract from papers left join venue on papers.id = venue.id left join authors on papers.id = authors.id left join year on papers.id = year.id;
""")

# my_table=pd.read_sql("select papers.id, title, mainauthor, coauthor, year, venue, abstract from papers left join venue on papers.id = venue.id left join mainauthor on papers.id = mainauthor.id left join year on papers.id = year.id left join coauthors on papers.id = coauthors.id order by papers.id ;", conn)
# print(my_table)

# question1
# my_table_1=pd.read_sql("select referenceid, referenceslist.id, title, mainauthor, coauthor, year, venue, abstract from referenceslist left join venue on referenceslist.id = venue.id left join mainauthor on referenceslist.id = mainauthor.id left join year on referenceslist.id = year.id left join coauthors on referenceslist.id = coauthors.id left join papers on referenceslist.id = papers.id order by referenceid;", conn)
# print(my_table_1)

# question 2
# my_table_2=pd.read_sql("select referenceslist.id, referenceid, title, mainauthor, coauthor, year, venue, abstract from referenceslist left join venue on referenceid = venue.id left join mainauthor on referenceid = mainauthor.id left join year on referenceid = year.id left join coauthors on referenceid = coauthors.id left join papers on referenceid = papers.id order by referenceslist.id;", conn)
# print(my_table_2)

# question 3
# my_table_3=pd.read_sql("select R2.referenceid, R1.id, title, mainauthor, coauthor, year, venue, abstract from referenceslist as R1 inner join referenceslist as R2 on R1.referenceid = R2.id left join venue on R1.id = venue.id left join papers on R1.id = papers.id left join mainauthor on R1.id = mainauthor.id left join coauthors on R1.id = coauthors.id left join year on R1.id = year.id order by R2.referenceid;",conn)
# print(my_table_3)

# question 3 trail
my_table_3 = pd.read_sql("select R2.referenceid as X, R1.id as Z, title, venue, authorname, year, abstract from referenceslist as R1 inner join referenceslist as R2 on R1.referenceid = R2.id left join information on R1.id = information.id order by X;", conn)
print(my_table_3)

# question 4
# my_table_4=pd.read_sql("SELECT referenceid, COUNT(referenceid) FROM referenceslist GROUP BY referenceid HAVING COUNT(referenceid) >= 1 ORDER BY COUNT(referenceid) DESC limit 20;", conn)
# print(my_table_4)


# question 5
# cur.execute("""
# SELECT id, coauthor as author into table temp_table FROM coauthors;
# """)
# cur.execute("""
#   insert into temp_table select c1.id as id, c1.mainauthor as author from mainauthor as c1 where c1.mainauthor != '';
# """)

# print(pd.read_sql("SELECT c1.author, c2.author, count(c1.author) from temp_table as c1 inner join temp_table as c2 on c1.id = c2.id where c1.author < c2.author group by c1.author, c2.author having count(c1.author) > 1;", conn))

cur.execute("""
    drop table information;
""")


