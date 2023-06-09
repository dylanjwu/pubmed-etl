# CS5200 - Spring 2023
# Dylan Wu
# Date: 04-20-23

# Objective of this file: Extract, transform, load XML data from document and load data into sql tables

hasPacman <- require(pacman)

if (!hasPacman) install.packages("pacman")
pacman::p_load(sqldf, xml2, hash, xml2, digest, fastmap, stringr)
options(sqldf.driver = 'SQLite')


## CONSTANTS AND INITIALIZATIONS
seasons = c("Spring", "Fall", "Winter", "Summer")

months = c('jan', 'feb', 'mar', 'apr', 'may', 'jun', 
           'jul', 'aug', 'sep', 'oct', 'nov', 'dec')

month_map = hash()
for (i in 1:length(months)) {
  month = months[i]
  month_map[[month]] = i
}

fields = c(
    'Article',
    'PMID',
    'ArticleTitle',
    'AuthorList',
    'CompleteYN',
    'ISSN',
    'IssnType',
    'Title',
    'ISOAbbreviation',
    'JournalIssue',
    'CitedMedium',
    'Year',
    'Issue',
    'Month',
    'Day',
    'MedlineDate',
    'Season',
    'Volume'
  )

author_fields = c('LastName', 'ForeName', 
                  'Initials', 'Suffix', 
                  'AffiliationInfo', 'ValidYN', 
                  'CollectiveName')

#### UTILITY/HELPER FUNCTIONS ####
  
# helper for parsing medline dates
get_months_avg <- function(months) {
  if (length(months) > 1) {
    if (months[1] < months[2]) {
      month = round(mean(months), 0)
    } else {
      month = round((months[1] + (12-sum(months))/2) %% 13, 0)
    }
  } else {
    month = ifelse(length(months) < 1, NA, months[1])
  }
  return (month)
}
  
# parses dates of the form = 
# c('1976 Dec-1977 Jan', '1977 Jan-Feb', '1976-1977 Winter', '1976-1977', '1977', '1977 Dec 24-31')
# returns mapping for month, year, day, season
# Format of dates is simply to have a column for month, day, year, season -- and to extract that info
# from medline dates when applicable by averaging values of year/day/month approximations
parse_medline_date <- function(date) {
  s = unlist(strsplit(date, split = '[- ]+')) # split w/ delimiters: - and space
  months = c() 
  season = NA
  years = c()
  days = c()
  for (el in s){
    if (!is.null(month_map[[tolower(el)]])) {
      months = c(months, as.integer(month_map[[tolower(el)]]))
    } else if (el %in% seasons) {
      season = el
    } else if (str_length(el) == 4) {
       years = c(years, as.integer(el))
    } else {
      days = c(days, as.integer(el))
    }
  }
  
  return (list(month=get_months_avg(months), 
               year=round(mean(years), 0), 
               day=ifelse(length(days) > 0, round(mean(days), 0), NA), 
               season=season
               ))
}


get_hash_key <- function(vector) {
  return (digest(vector, serialize = TRUE))
}

initialized_hash_map = function(fields) {
  h = hash()
  for (field in fields){
    h[[field]] = NA
  }
  return (h)
}

# dfs on article node, returning hashmap of node names to values (and node name to children)
# i found that doing it this way was way faster than using xpath for each node 
# this entire process takes about 7 mins, whereas the other way took hours
get_nodes_map = function(article) {
  stack = faststack()
  stack$push(article)
  h = initialized_hash_map(fields)
  
  while (stack$size() > 0) {
    
    curr = stack$pop()
    children = xml_children(curr)
    
    name = xml_name(curr)
    
    # get attributes
    if (name == 'ISSN'){
      h[['IssnType']] = xml_attr(curr, 'IssnType')
    } else if (name == 'AuthorList') {
      h[['CompleteYN']] = xml_attr(curr, 'CompleteYN')
    } else if (name == 'Article') {
      h[['PMID']] = xml_attr(curr, 'PMID')
    } else if (name == 'JournalIssue') {
      h[['CitedMedium']] = xml_attr(curr, 'CitedMedium')
    }
    
    if (length(children) > 0) {
      h[[name]] = curr
      if (name == 'AuthorList') next # process child author nodes later, from AuthorList node
      for (node in children) {
        stack$push(node)
      }
    } else { 
      h[[name]] = xml_text(curr)
    }
  }
  return (h)
}

# iterate through author children -- returns vector of hashmaps 
# each containing the name-value mapping of each of the author's attributes
get_authors = function(authorlist) {
  authors = c() # list of hashmaps
  for (author in xml_children(authorlist)) {
    h = initialized_hash_map(author_fields)
    h[['ValidYN']] = xml_attr(author, 'ValidYN')
    for (child in xml_children(author)) {
      h[[xml_name(child)]] = xml_text(child)
    }
    authors = c(authors, h)
  }
  return (authors)
}

# END OF HELPER FUNCTIONS


# this function does most of the work -- traverses the xml, adds content to different vectors,
# keeps track of unique values for tables using hash maps and counters
# finally, merges the different vectors into tables and writes them to the database (existing schema)
# that is, tables will only be written if complete xml document has been extracted
# (this entire program takes about 7 minutes to run on average)
load_all <- function(dom) {
  
  article_ids_for_authors = c()
  author_ids = c()
  
  # seasons table vectors
  seasons = c()
  
  # issn types table vectors
  issn_types = c()
  
  # Journal table vectors
  issn_type_ids = c()
  issns = c()
  journal_titles = c()
  iso_abbrevs = c()
  
  # cited mediums table vectors
  cited_mediums = c()
  
  # JournalIssue table vectors
  cited_medium_ids = c()
  journal_ids = c()
  issue_titles = c()
  pub_years = c()
  pub_months = c()
  pub_days = c()
  season_ids <- c()
  volumes = c()
  
  # Article table vectors
  journal_issue_ids = c()
  article_titles = c()
  complete_author_list_yns = c() 
  
  # Author table vectors
  lastnames=c()
  forenames=c()
  suffixes=c()
  initialss=c()
  affiliations=c()
  affiliation_ids=c()
  valid_yns=c()
  collective_names = c()
  
  # following is set of hashmaps and counters for preserving uniques 
  # and creating ids for tables 
  author_hash <- hash()
  author_id = 1
  
  affil_hash <- hash()
  curr_affil_id = 1
  
  journal_hash = hash() 
  curr_journal_id = 1
  
  season_hash = hash()
  curr_season_id <- 1
  
  journal_issue_hash = hash()
  curr_journal_issue_id = 1
  
  cited_medium_hash = hash()
  curr_cited_medium_id = 1
  
  issn_type_hash = hash()
  curr_issn_type_id = 1
  
  articles = xml_find_all(dom, '//Publications/Article')
  
  progress = 1 
  
  for (article in articles) {
    
    # track progress (number of articles processed)
    if (progress %% 1000 == 0) print(paste("progress:", progress))
    progress = progress + 1
    
    nodes = get_nodes_map(article)
    
    article_node = nodes[['Article']]
    article_id = nodes[['PMID']]
    
    article_title = nodes[['ArticleTitle']]
    article_titles = c(article_titles, article_title)
    author_list = nodes[['AuthorList']]
    complete_author_list_yn = ifelse(!is.na(author_list) && nodes[['CompleteYN']] == 'Y', T, F)
    complete_author_list_yns = c(complete_author_list_yns, complete_author_list_yn)
    issn = nodes[['ISSN']]
    issn_type = nodes[['IssnType']]
    journal_title = nodes[['Title']]
    iso_abbrev = nodes[['ISOAbbreviation']]
    journal_issue = nodes[['JournalIssue']]
    cited_medium = nodes[['CitedMedium']]
    pub_year = nodes[['Year']]
    issue_title = nodes[['Issue']]
    pub_month = nodes[['Month']]
    pub_day = nodes[['Day']]
    medline_date = nodes[['MedlineDate']]
    season = nodes[['Season']]
    volume = nodes[['Volume']]
    
    # issn type
    issn_type_id <- ifelse(is.na(issn_type) || is.null(issn_type_hash[[issn_type]]),
                           NA, issn_type_hash[[issn_type]])
    if (!is.na(issn_type) && is.na(issn_type_id)) {
      issn_type_hash[[issn_type]] = curr_issn_type_id
      issn_type_id = curr_issn_type_id
      issn_types = c(issn_types, issn_type)
      curr_issn_type_id = curr_issn_type_id + 1
    }
    
    # cited_medium
    cited_medium_id <- ifelse(is.na(cited_medium) || is.null(cited_medium_hash[[cited_medium]]), 
                              NA, cited_medium_hash[[cited_medium]])
    if (!is.na(cited_medium) && is.na(cited_medium_id)) {
      cited_medium_hash[[cited_medium]] = curr_cited_medium_id
      cited_medium_id = curr_cited_medium_id
      cited_mediums = c(cited_mediums, cited_medium)
      curr_cited_medium_id = curr_cited_medium_id + 1
    }
    
    # journal
    journal_id = journal_hash[[journal_title]]
    if (is.null(journal_id)) {
      journal_hash[[journal_title]] = curr_journal_id
      journal_id = curr_journal_id
      
      issns = c(issns, issn)
      journal_titles = c(journal_titles, journal_title)
      iso_abbrevs = c(iso_abbrevs, iso_abbrev)
      issn_type_ids = c(issn_type_ids, issn_type_id) 
      
      curr_journal_id = curr_journal_id + 1
    }
    
    # season
    season_id <- ifelse(is.na(season) || is.null(season_hash[[season]]), NA, season_hash[[season]])
    if (!is.na(season) && is.na(season_id)) {
      season_hash[[season]] = curr_season_id
      season_id <- curr_season_id
      seasons = c(seasons, season)
      curr_season_id = curr_season_id + 1
    }
    
    # parsing medline date -- into month/season/day/year, components -- take averages if multiples
    if (!is.na(medline_date)) {
      parsed_date = parse_medline_date(medline_date)
      pub_year = parsed_date[['year']]
      pub_month = parsed_date[['month']]
      pub_day = parsed_date[['day']]
      season = parsed_date[['season']]
    } else if (!is.na(pub_month)) {
      pub_month = ifelse(tolower(pub_month) %in% names(month_map), 
                         month_map[[tolower(pub_month)]], as.integer(pub_month))
    }
    
    # journal issue
    journal_issue_full = get_hash_key(c(journal_id, volume, issue_title))
    journal_issue_id = journal_issue_hash[[journal_issue_full]]
    if (is.null(journal_issue_id)) {
      journal_issue_hash[[journal_issue_full]] = curr_journal_issue_id
      journal_issue_id = curr_journal_issue_id
      
      # add to requisite vectors for journal dataframe/table
      pub_years = c(pub_years, pub_year)
      pub_months = c(pub_months, pub_month)
      issue_titles = c(issue_titles, issue_title)
      pub_days = c(pub_days, pub_day)
      cited_medium_ids = c(cited_medium_ids, cited_medium_id)
      journal_ids = c(journal_ids, journal_id)
      season_ids <- c(season_ids, season_id)
      volumes = c(volumes, volume)
      
      curr_journal_issue_id = curr_journal_issue_id + 1
    }
    
    # for articles table, each article has journal_issue id
    journal_issue_ids = c(journal_issue_ids, journal_issue_id) 
    
    # if no authors, skip to next iteration
    if (is.na(author_list)) {
      next;
    }
    
    # authors
    for (author in get_authors(author_list)) {
      
        collective_name = author[['CollectiveName']]
        affiliation = author[['AffiliationInfo']]
        
        # create hashkey for collective name or author (either-or)
        if (!is.na(collective_name)) {
          hashkey = get_hash_key(c(collective_name))
        } else {
          hashkey = get_hash_key(c(author[['LastName']], author[['ForeName']], 
                                   author[['Initials']], author[['Suffix']]))
        }
        
        # get affiliation for affiliations table
        affil_id = ifelse(is.na(affiliation) || is.null(affil_hash[[affiliation]]), 
                          NA, affil_hash[[affiliation]])
        if (!is.na(affiliation) && is.na(affil_id)) {
          affil_hash[[affiliation]] = curr_affil_id
          affil_id = curr_affil_id
          affiliations = c(affiliations, affiliation)
          curr_affil_id = curr_affil_id + 1
        }
        
        # get author for authors table
        if (is.null(author_hash[[hashkey]])) {
          author_hash[[hashkey]] = author_id
          
          # add to vectors for dataframe
          lastnames = c(lastnames, author[['LastName']])
          forenames = c(forenames, author[['ForeName']])
          suffixes = c(suffixes, author[['Suffix']])
          affiliation_ids = c(affiliation_ids, affil_id)
          valid_yn = author[['ValidYN']]
          valid_yns = c(valid_yns, valid_yn)
          initialss = c(initialss, author[['Initials']])
          collective_names = c(collective_names, collective_name)
          
          author_id = author_id + 1
        }
        
        # add author and article ids for AuthorArticle junction dataframe/table
        author_ids = c(author_ids, author_hash[[hashkey]])
        article_ids_for_authors = c(article_ids_for_authors, article_id) 
        
      }
  }
  
  # construct tables
  author_df = data.frame(id=c(1:length(forenames)), collective_name=collective_names, 
                         forename=forenames, lastname=lastnames, initials=initialss, 
                         affiliation_id=affiliation_ids, valid_yn=valid_yns, suffix=suffixes)
  dbWriteTable(conn, 'Author', author_df, append=TRUE, row.names = FALSE)
  
  affil_df = data.frame(id=c(1:length(affiliations)), name=affiliations)
  dbWriteTable(conn, 'Affiliation', affil_df, append=TRUE, row.names = FALSE)
  
  season_table = data.frame(id=c(1:length(seasons)), name=seasons)
  dbWriteTable(conn, 'Season', season_table, append=TRUE, row.names = FALSE)
  
  cited_mediums_table = data.frame(id=c(1:length(cited_mediums)), name=cited_mediums)
  dbWriteTable(conn, 'CitedMedia', cited_mediums_table, append=TRUE, row.names = FALSE)
  
  issn_types_table = data.frame(id=c(1:length(issn_types)), name=issn_types)
  dbWriteTable(conn, 'IssnType', issn_types_table, append=TRUE, row.names = FALSE)
  
  journal_table = data.frame(id=c(1:length(issns)), issn_type_id=as.integer(issn_type_ids), 
                             issn=issns, title=journal_titles, iso_abbreviation=iso_abbrevs)
  dbWriteTable(conn, 'Journal', journal_table, append=TRUE, row.names = FALSE)
  
  journal_issue_table = data.frame(id=c(1:length(journal_ids)), cited_medium_id=as.integer(cited_medium_ids), 
                                   journal_id=as.integer(journal_ids), issue_title=issue_titles, pub_month=pub_months, 
                                   pub_year=pub_years, pub_day=pub_days, season_id=as.integer(season_ids), 
                                   volume=as.integer(volumes))
  dbWriteTable(conn, 'JournalIssue', journal_issue_table,append=TRUE, row.names = FALSE)
  
  article_table = data.frame(id=c(1:length(article_titles)), journal_issue_id=as.integer(journal_issue_ids),
                             title=article_titles, complete_author_list_yn=complete_author_list_yns)
  dbWriteTable(conn, 'Article', article_table, append=TRUE, row.names = FALSE)
  
  article_author_table = data.frame(id=c(1:length(author_ids)), article_id=as.integer(article_ids_for_authors),
                                    author_id=as.integer(author_ids))
  dbWriteTable(conn, 'ArticleAuthor', article_author_table, append=TRUE, row.names = FALSE)
}


create_schema <- function() {
  res = dbSendQuery(conn, "CREATE TABLE Affiliation (
    id INTEGER PRIMARY KEY, 
    name text NOT NULL UNIQUE
  )");
  
  dbClearResult(res)
  
  res = dbSendQuery(conn, "CREATE TABLE Author (
    id INTEGER PRIMARY KEY,
    collective_name TEXT,
    lastname TEXT,
    forename TEXT,
    initials TEXT,
    suffix TEXT,
    affiliation_id INTEGER,
    valid_yn boolean NOT NULL,
    FOREIGN KEY (affiliation_id) REFERENCES Affiliation (id)
  )")
  
  dbClearResult(res)
  
  res = dbSendQuery(conn, "CREATE TABLE CitedMedia (
    id INTEGER PRIMARY KEY,
    name text NOT NULL UNIQUE
  )")
  
  dbClearResult(res)
  
  res = dbSendQuery(conn, "CREATE TABLE IssnType (
    id INTEGER PRIMARY KEY,
    name text NOT NULL UNIQUE
  )")
  
  dbClearResult(res)
  
  res = dbSendQuery(conn, "CREATE TABLE Journal (
    id INTEGER PRIMARY KEY,
    issn_type_id INTEGER,
    issn INTEGER,
    title TEXT,
    iso_abbreviation TEXT,
    FOREIGN KEY (issn_type_id) REFERENCES IssnTypes (id)
  )")
  
  dbClearResult(res)
  
  res = dbSendQuery(conn, "CREATE TABLE Season (
    id INTEGER PRIMARY KEY,
    name text NOT NULL UNIQUE
  )")
  
  dbClearResult(res)
  
  res = dbSendQuery(conn, "CREATE TABLE JournalIssue (
    id INTEGER PRIMARY KEY,
    cited_medium_id INTEGER,
    journal_id INTEGER NOT NULL,
    issue_title TEXT,
    pub_year INT,
    pub_month TINYINT,
    pub_day TINYINT,
    season_id INTEGER,
    volume INTEGER,
    FOREIGN KEY (season_id) REFERENCES Seasons (id),
    FOREIGN KEY (cited_medium_id) REFERENCES CitedMedia (id),
    FOREIGN KEY (journal_id) REFERENCES Journal (id)
  )")
  
  dbClearResult(res)
  
  res = dbSendQuery(conn, "CREATE TABLE Article (
    id INTEGER PRIMARY KEY,
    journal_issue_id INTEGER NOT NULL,
    title TEXT NOT NULL,
    complete_author_list_yn BOOLEAN DEFAULT TRUE,
    FOREIGN KEY (journal_issue_id) REFERENCES JournalIssue (id)
  )")
  
  dbClearResult(res)
  
  res = dbSendQuery(conn, "CREATE TABLE ArticleAuthor (
    id INTEGER PRIMARY KEY,
    author_id INTEGER NOT NULL,
    article_id INTEGER NOT NULL,
    FOREIGN KEY (author_id) REFERENCES Author (id),
    FOREIGN KEY (article_id) REFERENCES Article (id)
  )")
  
  dbClearResult(res)
  
  print("schema created")
}

clearDb <- function(conn) {
  tables = c('IssnType', 'Season', 'Journal', 'JournalIssue', 'CitedMedia', 
             'Author', 'ArticleAuthor', 'Affiliation', 'Article')
  for (table in tables) {
    res = dbSendQuery(conn, paste('DROP TABLE IF EXISTS', table))
    dbClearResult(res)
  }
}


# runs entire ETL process
main <- function() {
  
  fpath = 'pubmed.db'
  conn <- dbConnect(RSQLite::SQLite(), fpath)
  clearDb(conn) # clear db for a fresh start
  create_schema() # construct tables, primary/foreign key relationships, etc
  
  # get xml file from aws s3
  xmlFile <- 'https://cs5200-practicum2.s3.amazonaws.com/pubmed22n0001-tf.xml'
  
  dom <- read_xml(xmlFile, validate = T)
  
  load_all(dom) # load all data into sqlite tables from xml doc
  
  dbDisconnect(conn)

}

main()
