import psycopg2
import pandas as pd
conn = psycopg2.connect(host="localhost", dbname="assgn2", user="postgres", password="myPassword")
cur=conn.cursor()

# cur.execute("""
# 	select * from papers left join venue
# 	on papers.id = venue.id;
# """)

# my_table=pd.read_sql("select papers.id, title, mainauthor, coauthor, year, venue, abstract from papers left join venue on papers.id = venue.id left join mainauthor on papers.id = mainauthor.id left join year on papers.id = year.id left join coauthors on papers.id = coauthors.id order by papers.id ;", conn)
# print(my_table)

# question1
# my_table_1=pd.read_sql("select referenceid, referenceslist.id, title, mainauthor, coauthor, year, venue, abstract from referenceslist left join venue on referenceslist.id = venue.id left join mainauthor on referenceslist.id = mainauthor.id left join year on referenceslist.id = year.id left join coauthors on referenceslist.id = coauthors.id left join papers on referenceslist.id = papers.id order by referenceid;", conn)
# print(my_table_1)

# question 2
# my_table_2=pd.read_sql("select referenceslist.id, referenceid, title, mainauthor, coauthor, year, venue, abstract from referenceslist left join venue on referenceid = venue.id left join mainauthor on referenceid = mainauthor.id left join year on referenceid = year.id left join coauthors on referenceid = coauthors.id left join papers on referenceid = papers.id order by referenceslist.id;", conn)
# print(my_table_2)

# question 3
# my_table_3=pd.read_sql("select R2.referenceid, R1.id, title, mainauthor, coauthor, year, venue, abstract from referenceslist as R1 left join referenceslist as R2 on R1.referenceid = R2.id left join venue on R1.id = venue.id left join papers on R1.id = papers.id left join mainauthor on R1.id = mainauthor.id left join coauthors on R1.id = coauthors.id left join year on R1.id = year.id order by R2.referenceid limit 20",conn)
# print(my_table_3)

# question 4
# my_table_4=pd.read_sql("SELECT referenceid, COUNT(referenceid) FROM referenceslist GROUP BY referenceid HAVING COUNT(referenceid) >= 1 ORDER BY COUNT(referenceid) DESC limit 20;", conn)
# print(my_table_4)

# question 5
# my_table_5=pd.read_sql("SELECT coauthor, count(coauthor) FROM coauthors GROUP BY coauthor HAVING COUNT(coauthor)>1;",conn)
# print(my_table_5)




# (select papers.id, title, abstract, venue, mainauthor from papers left join venue on papers.id = venue.id order by papers.id) x left join mainauthor on x.id = mainauthor.id