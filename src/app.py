import sqlite3
import requests
import pandas as pd
from bs4 import BeautifulSoup


#Accessing the data
url = 'https://www.macrotrends.net/stocks/charts/TSLA/tesla/revenue'
response = requests.get(url)
html_data = response.text
soup = BeautifulSoup(html_data,features='html.parser')


#Looking for the table we need
all_tables = soup.find_all('table')
print(all_tables)

classes = []
for i in all_tables:
    class_i = i.get('class')
    classes.append(class_i)
print(classes)

#By class i cannot differentiate the table i want
#Instead, look for the index of the table that contains the string 'Tesla Quarterly Revenue'

index = 0
for i, table in enumerate(all_tables):
    if ('Tesla Quarterly Revenue' in str(table)):
        index = i
quarterly_data = all_tables[index]
data_body = quarterly_data.find('tbody')
rows = data_body.find_all('tr') #The rows format is Date - Revenue


#Creating the dataframe
tesla_revenue = pd.DataFrame(columns=['Date','Revenue'])

for row in rows:
    data = row.find_all('td')
    if len(data)>0:
        date = data[0].text
        revenue = data[1].text.replace('$','').replace(',','')
        # tesla_revenue.concat({'Date':date , 'Revenue':revenue}, ignore_index=True)
        tesla_revenue = pd.concat([tesla_revenue, pd.DataFrame({'Date':[date],'Revenue':[revenue]})], ignore_index=True, axis=0)

print(tesla_revenue)
tesla_revenue.info()


#Checking for na values and empty values
any_na = tesla_revenue.isnull().values.any()
if any_na:
    print('There are na values in the dataframe, so we drop them')
    tesla_revenue.dropna()
else:
    print('There are no na values in the dataframe')

any_empty_date = tesla_revenue.loc[tesla_revenue['Date'] == '']
if len(any_empty_date)>0:
    print('There are empty date values, so we proceed to eliminate those rows')
    tesla_revenue = tesla_revenue[tesla_revenue['Date'] != '']
else:
    print('There are no empty date values')

any_empty_revenue = tesla_revenue.loc[tesla_revenue['Revenue'] == '']
if len(any_empty_revenue)>0:
    print('There are empty revenue values, so we proceed to eliminate those rows')
    tesla_revenue = tesla_revenue[tesla_revenue['Revenue'] != '']
else:
    print('There are no empty date values')


#Converting the dataframe into a list of tuples
records = tesla_revenue.to_records(index=False)
tesla_revenue_list = list(records)


#Working with the database
connection = sqlite3.connect('Tesla.db')
cursor = connection.cursor()

sql_create_teslarev_table = '''CREATE TABLE IF NOT EXISTS tesla_revenues
                                (Date,Revenue)'''

cursor.execute(sql_create_teslarev_table)

for i, tuple in enumerate(tesla_revenue_list):
    sql_insert =''' INSERT INTO tesla_revenues
              VALUES(?,?) '''
    args = tuple
    cursor.execute(sql_insert,args)

connection.commit()

data = cursor.execute('SELECT * FROM tesla_revenues')

for i in data:
    print(i)

cursor.execute('DELETE FROM tesla_revenues;')
connection.commit()


