# -*- coding: utf-8 -*-

import pandas as pd
from bs4 import BeautifulSoup
import requests
import re
import string
from sqlalchemy import create_engine
import os.path
import datetime


path=os.path.join(os.path.dirname(__file__))

    
#define func - get player basic information 
def get_player_list():
    player_name=[]
    player_url=[]
    player_code=[]
    first_letter=list(string.ascii_lowercase)
    for i in first_letter:
        nba = requests.get('https://www.basketball-reference.com/players/'+i+'/')
        soup = BeautifulSoup(nba.text, 'html.parser')
        a_tags = soup.find_all('a', href=re.compile("/players/"+i+"/*")) 
        #get player name
        for tag in a_tags:
            player_name.append(tag.string)
        #get player_url
        for tag in a_tags:
            player_url.append(tag.get('href'))
    #get player_id
    for i in player_url:
        player_code.append(i[11:i.find('.')])
    
    height_list=[]
    weight_list=[]
    birth_day_list=[]
    college_list=[]
    country_list=[]
    draft_team_list=[]
    draft_pick_list=[]
    draft_year_list=[]

    for i in player_url:
        player_detail=requests.get('https://www.basketball-reference.com'+ i)# define target url
        detail_soup = BeautifulSoup(player_detail.text, 'html.parser')
        height = detail_soup.find_all('span', itemprop="height")# get height
        weight = detail_soup.find_all('span', itemprop="weight")# get weight
        birth_day= detail_soup.find_all('span', itemprop="birthDate")#get birthday
        college = detail_soup.find_all('a', href=re.compile("/friv/colleges.fcgi*"))#get college
        country = detail_soup.find_all('a', href=re.compile("/friv/birthplaces.fcgi*"))#get birthplace
        draft = detail_soup.find_all('a', href=re.compile("/draft.html"))#get draft information
        draft_year = detail_soup.find_all('a', href=re.compile("/draft/NBA"))#get draft year
        try:
            height_list.append(height[0].string)
        except IndexError:
            height_list.append('none')
        
        try:
            weight_list.append(weight[0].string)
        except IndexError:
            weight_list.append('none')    
            
        try:
            birth_day_list.append(birth_day[0].get('data-birth'))
        except IndexError:
            birth_day_list.append('none')    
            
        try:
            college_list.append(college[0].string)
        except IndexError:
            college_list.append('none')   
            
        try:
            country_list.append(country[0].string)
        except IndexError:
            country_list.append('none')    
            
        try:
            draft_team_list.append(draft[0].string)    
        except IndexError:
            draft_team_list.append('none')    
            
        try:
            draft_pick_list.append(draft[0].next_sibling)
        except IndexError:
            draft_pick_list.append('none')   
            
        try:
            draft_year_list.append(draft_year[0].string)
        except IndexError:
            draft_year_list.append('none')  
            
    return pd.DataFrame(
            {
                    'player_name':player_name,
                    'player_url':player_url,
                    'player_code':player_code,
                    'height':height_list,
                    'weight':weight_list,
                    'birth_day':birth_day_list,
                    'college':college_list,
                    'country':country_list,
                    'draft_team':draft_team_list,
                    'draft_pick':draft_pick_list,
                    'draft_year_raw':draft_year_list               
                    }
            ) # return as dataframe

#define func - get player annual stat 
def get_annual(url):
    table_avr=pd.read_html(url)[0]
    table_avr=table_avr[table_avr['Rk']!='Rk']
    nba = requests.get(url)
    soup = BeautifulSoup(nba.text, 'html.parser')
    a_tags = soup.find_all('td')    
    link_tag=list() 
    #get player_id
    for tag in a_tags:
      link_tag.append(tag.get('data-append-csv'))
    link_tag=list(filter(None, link_tag))    
    table_avr['player_id']=link_tag
    return table_avr #return dataframe


#define func - re-org draft rounds
def draft_round(draft):
    if draft=='none' or draft==', ':
        r='not draft'
    else:
        r=draft[draft.find(', ')+2:draft.find(' (')]
    return r

#define func - re-org draft picks
def draft_pick(draft):
    if draft=='none' or draft==', ':
        r='not draft'
    else:
        r=draft[draft.find(' (')+2:draft.find(' pick')]
    return r

#define func - set draft year
def draft_year(year):
    return year[:4]

#get data
if __name__ == "__main__":
    player=get_player_list()
    player['draft_round']=player['draft_pick'].apply(draft_round)
    player['draft_order']=player['draft_pick'].apply(draft_pick)
    player['draft_year']=player['draft_year_raw'].apply(draft_year)
    player['modefied_date']=datetime.date.today()
    
    #get per36 annual stats        
    table_per36=get_annual('https://www.basketball-reference.com/leagues/NBA_1950_per_game.html')
    table_per36['year']=1950
    for i in range(1951,datetime.date.today().year+1):
        table_add=get_annual('https://www.basketball-reference.com/leagues/NBA_' + str(i) + '_per_game.html')
        table_add['year']=i
        table_per36=pd.concat([table_per36, table_add], axis = 0, ignore_index=True)
    table_per36['modefied_date']=datetime.date.today()

    #get advanced annual stats    
    table_adv=get_annual('https://www.basketball-reference.com/leagues/NBA_1980_advanced.html')
    table_adv['year']=1980
    for i in range(1981,datetime.date.today().year+1):
        table_add=get_annual('https://www.basketball-reference.com/leagues/NBA_' + str(i) + '_advanced.html')
        table_add['year']=i
        table_adv=pd.concat([table_adv, table_add], axis = 0, ignore_index=True)
    table_adv['modefied_date']=datetime.date.today()#資料增補日期
        
    #storage
    engine = create_engine('sqlite:///'+path+'\\nba_db', echo=False)#set sqlite location
    player.to_sql('player_list',con=engine,if_exists='replace')#basic information to DB
    table_per36.to_sql('table_per36',con=engine,if_exists='replace')#per36 stat to DB
    table_adv.to_sql('table_adv',con=engine,if_exists='replace')#advanced stats to DB
    
