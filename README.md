# NBA_Reference_Scrapy
Get player information and annual stats from basketball-reference website, and storage data in sqlite.

=======================================
Just RUN the player_list_20190725.py and wait for a couple of hours,then you'll get follow data(as sqlite db):
1. player basic information: including height, weight, birthday, college, draft information ,etc.(from 1951 to today)
2. per36 annual stats: from 1951 to today
3. advanced annual stats: from 1980 to today

#lib required:
pandas 
BeautifulSoup
requests
re
string
sqlalchemy 
os.path
datetime
