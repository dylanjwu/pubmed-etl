# CS5200 - Spring 2023
# Dylan Wu
# Date: 04-20-23

if (!hasPacman) install.packages("pacman")
pacman::p_load(RMySQL)
options(sqldf.driver = 'SQLite')
fpath = './pubmed.db'
sqlite_conn <- dbConnect(RSQLite::SQLite(), fpath)
mysql_conn <- dbConnect(
  RMySQL::MySQL(),
  db = 'cs5200_db',
  user = 'admin',
  port = 3306,
  host = '****',
  password = '****'
)


# This query contains a CTE called all_journals_info which aggregates all the relevant tables with joins
# and calculates quarters by months, it then creates a CTE called totals which gathers (groups)
# the info needed to get the total number of journals for the entire time period of 4 years
# these two tables are then used in the final select expression which groups by journal_id and year
# and then calculates the per-quarter and per-month facts for number of articles and authors per year
# 'all_journals_info' is selected from, to which 'totals' is joined to gather all of the relevant data

df = dbGetQuery(sqlite_conn, "WITH all_journals_info AS (
  	SELECT
  		j.id AS journal_id,
  		j.title AS journal_title,
  		a.id AS article_id,
  		CASE WHEN pub_month BETWEEN 10 AND 12 THEN 4
  		WHEN pub_month BETWEEN 7 AND 9 THEN 3
  		WHEN pub_month BETWEEN 4 AND 6 THEN 2
  		WHEN pub_month BETWEEN 1 AND 3 THEN 1
  		ELSE
  			NULL
  		END AS quarter,
  		pub_year,
  		pub_month,
  		aa.author_id AS author_id
  	FROM
  		Article a
  		JOIN JournalIssue js ON (a.journal_issue_id = js.id)
  		JOIN Journal j ON (js.journal_id = j.id)
  		JOIN ArticleAuthor aa ON (a.id = aa.article_id)
  		JOIN Author auth ON (auth.id = aa.author_id)
  ),
  totals AS (
  	SELECT
  		j.id AS journal_id,
  		COUNT(a.id) AS articles_total
  	FROM
  		Article a
  		JOIN JournalIssue js ON (a.journal_issue_id = js.id)
  		JOIN Journal j ON (js.journal_id = j.id)
  	GROUP BY
  		journal_id
  )
  SELECT
  	aji.journal_id,
  	journal_title,
  	pub_year,
  	t.articles_total AS articles_total,
  	COUNT(DISTINCT article_id) AS articles_year,
  	COUNT(DISTINCT CASE WHEN quarter = 1 THEN
  			article_id
  		END) AS articles_q1,
  	COUNT(DISTINCT CASE WHEN quarter = 2 THEN
  			article_id
  		END) AS articles_q2,
  	COUNT(DISTINCT CASE WHEN quarter = 3 THEN
  			article_id
  		END) AS articles_q3,
  	COUNT(DISTINCT CASE WHEN quarter = 4 THEN
  			article_id
  		END) AS articles_q4,
  	COUNT(DISTINCT CASE WHEN quarter = 1 THEN
  			author_id
  		END) AS authors_q1,
  	COUNT(DISTINCT CASE WHEN quarter = 2 THEN
  			author_id
  		END) AS authors_q2,
  	COUNT(DISTINCT CASE WHEN quarter = 3 THEN
  			author_id
  		END) AS authors_q3,
  	COUNT(DISTINCT CASE WHEN quarter = 4 THEN
  			author_id
  		END) AS authors_q4,
  	COUNT(DISTINCT CASE WHEN quarter = 1 THEN
  			author_id
  		END) AS authors_jan,
  	COUNT(DISTINCT CASE WHEN quarter = 2 THEN
  			author_id
  		END) AS authors_feb,
  	COUNT(DISTINCT CASE WHEN quarter = 3 THEN
  			author_id
  		END) AS authors_mar,
  	COUNT(DISTINCT CASE WHEN quarter = 4 THEN
  			author_id
  		END) AS authors_apr,
  	COUNT(DISTINCT CASE WHEN quarter = 5 THEN
  			author_id
  		END) AS authors_may,
  	COUNT(DISTINCT CASE WHEN quarter = 6 THEN
  			author_id
  		END) AS authors_jun,
  	COUNT(DISTINCT CASE WHEN quarter = 7 THEN
  			author_id
  		END) AS authors_jul,
  	COUNT(DISTINCT CASE WHEN quarter = 8 THEN
  			author_id
  		END) AS authors_aug,
  	COUNT(DISTINCT CASE WHEN quarter = 9 THEN
  			author_id
  		END) AS authors_sep,
  	COUNT(DISTINCT CASE WHEN quarter = 10 THEN
  			author_id
  		END) AS authors_oct,
  	COUNT(DISTINCT CASE WHEN quarter = 11 THEN
  			author_id
  		END) AS authors_nov,
  	COUNT(DISTINCT CASE WHEN quarter = 12 THEN
  			author_id
  		END) AS authors_dec
  FROM
  	all_journals_info AS aji
  	JOIN totals AS t ON (aji.journal_id = t.journal_id)
  GROUP BY
  	aji.journal_id,
  	pub_year
")

# write table to mysql database -- call table: JournalFacts
dbWriteTable(mysql_conn, 'JournalFacts', df, overwrite = T)

dbDisconnect(mysql_conn)

