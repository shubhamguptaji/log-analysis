import psycopg2
# imports psycopg2 module

DBNAME = "news"


article_views = open('article_views.txt', 'w+')
author_views = open('author_views.txt', 'w+')
errors = open('errors.txt', 'w+')


# gives top 3 most viewed  articles of all time
def problem1():
    db = psycopg2.connect(database = DBNAME)
    c = db.cursor()
    c.execute("""select articles.title,
              count(*) as veiws from
              log,articles where
              log.path = '/article/' ||
              articles.slug group by
              articles.title order
              by veiws desc limit 3;
              """)
    views = c.fetchall()
    for item in views:
        article_views.write('"' + str(item[0]) + '"' + ' - ' + str(item[1]) + ' views\n')
        db.close()


#gives the authors name with most views
def problem2():
    db = psycopg2.connect(database = DBNAME)
    c = db.cursor()
    c.execute("""select authors.name, count(*) as
                sum from log,articles,authors where
                log.path = '/article/' || articles.slug
                and authors.id=articles.author group
                by authors.name order by sum desc;
                """)
    authors = c.fetchall()
    for i in authors:
        author_views.write('"' + str(i[0]) + '" - ' + str(i[1]) + ' views\n')
    db.close()


#gives the %errors with date
def problem3():
    db = psycopg2.connect(database = DBNAME)
    c = db.cursor()
    c.execute("""create view totalrequests as
                select count(*) as total,
                date(time) from log group
                by date(time) order
                by date(time);""")
    c.execute("""create view errors as select
                count(*) as errors,date(time)
                from log where status =
                '404 NOT FOUND' group by date(time)
                order by date(time);""")
    c.execute("""create view percentage_errors as
                select (cast(errors.errors as float)
                /cast(totalrequests.total as float))
                *100 as percentage,totalrequests.date
                from totalrequests natural join errors;
                """)
    c.execute("""select * from percentage_errors where
                percentage > 1;""")
    percentage = c.fetchall()
    for item in percentage:
        errors.write(str(item[1]) + ' - ' + '%0.2f' % item[0] + '%errors\n')
    db.close()


problem1()
problem2()
problem3()

