import psycopg2
import pandas as pd
conn = psycopg2.connect(host="localhost", dbname="assgn2", user="postgres", password="myPassword")
cur=conn.cursor()



# question1
print("\n\n############################################### Question 1 ###################################################\n\n")
my_table_1=pd.read_sql("select referenceid as paperid, referenceslist.id as cited_by_paperid, title, mainauthor, coauthor, year, venue, abstract from referenceslist left join venue on referenceslist.id = venue.id left join mainauthor on referenceslist.id = mainauthor.id left join year on referenceslist.id = year.id left join coauthors on referenceslist.id = coauthors.id left join papers on referenceslist.id = papers.id order by referenceid;", conn)
print(my_table_1)




# question 2
print("\n\n############################################### Question 2 ###################################################\n\n")
my_table_2=pd.read_sql("select referenceslist.id as paperid, referenceid as cites_paperid, title, mainauthor, coauthor, year, venue, abstract from referenceslist left join venue on referenceid = venue.id left join mainauthor on referenceid = mainauthor.id left join year on referenceid = year.id left join coauthors on referenceid = coauthors.id left join papers on referenceid = papers.id order by referenceslist.id;", conn)
print(my_table_2)




# question 3
print("\n\n############################################### Question 3 ###################################################\n\n")
my_table_3=pd.read_sql("select R2.referenceid as paper1, R1.id as paper2 from referenceslist as R1 inner join referenceslist as R2 on R1.referenceid = R2.id order by R2.referenceid;",conn)
print(my_table_3)



# question 4
print("\n\n############################################### Question 4 ###################################################\n\n")
my_table_4=pd.read_sql("SELECT referenceid as paperid, COUNT(referenceid) FROM referenceslist GROUP BY referenceid HAVING COUNT(referenceid) >= 1 ORDER BY COUNT(referenceid) DESC limit 20;", conn)
print(my_table_4)



# question 5
print("\n\n############################################### Question 5 ###################################################\n\n")
cur.execute("""
SELECT id, coauthor as author into temp table temp_table FROM coauthors;
""")
cur.execute("""
  insert into temp_table select c1.id as id, c1.mainauthor as author from mainauthor as c1 where c1.mainauthor != '';
  create temp table temp_table_1 as select distinct id, author from temp_table;
""")

print(pd.read_sql("SELECT c1.author as author1, c2.author as author2, count(*) from temp_table_1 as c1 inner join temp_table_1 as c2 on c1.id = c2.id where c1.author < c2.author group by c1.author, c2.author having count(*) > 1;", conn))


# # question 6
print("\n\n############################################### Question 6 ###################################################\n\n")
cur.execute("""
    create temp table authors_0 as select id, mainauthor as authorname from mainauthor as c1 where c1.mainauthor != '';
    insert into authors_0(id, authorname) select id, coauthor as authorname from coauthors;
    create temp table authors as select distinct id, authorname from authors_0;
""")

cur.execute("""
    create temp table base_table as select R1.id as X, R2.id as Y, R2.referenceid as Z from referenceslist as R1 inner join referenceslist as R2 on R1.referenceid = R2.id;
    
    create temp table new_table as select X, Y, Z from base_table inner join referenceslist as R3 on R3.id = Z where  R3.referenceid =X union select X, Y, Z from base_table inner join referenceslist as R4 on R4.id = X where R4.referenceid =Z; 
    
    create temp table author_table as select A1.authorname as author1, A2.authorname as author2, A3.authorname as author3 
    from new_table 
    inner join authors as A1 on X = A1.id 
    inner join authors as A2 on Y = A2.id and A1.authorname != A2.authorname
    inner join authors as A3 on Z = A3.id and A3.authorname != A1.authorname and A3.authorname != A2.authorname;
    

    update author_table set author1 = author2, author2 = author1 where author1 > author2;
    update author_table set author2 = author3, author3 = author2 where author2 > author3;
    update author_table set author3 = author1, author1 = author3 where author3 > author1;
""")

my_table_6= pd.read_sql("select author1, author2, author3, count(*) from author_table  group by author1, author2, author3 order by count DESC", conn)
print(my_table_6)


conn.commit()
cur.close()
conn.close()

