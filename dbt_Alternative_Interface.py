import re
import http.client
import json
import pandas as pd
 


test = '''select *
from Intermediary_Model
order by C_CUSTKEY;
'''
list = re.split(r'\.\.\.', test)
print(list)
query = list[0]
print(query)




import http.client
import json

conn = http.client.HTTPSConnection("gc55340.west-us-2.azure.snowflakecomputing.com")
payload = json.dumps({
  "statement": query,
  "timeout": 60,
  "resultSetMetaData": {
    "format": "json"
  },
  "database": "DEMO_DB",
  "schema": "PUBLIC",
  "warehouse": "COMPUTE_WH",
  "parameters": {
    "MULTI_STATEMENT_COUNT": "0", 
    
  }
})
headers = {
  'Authorization': 'Bearer xxxxxxxxxxxxxxxxxxxx',
  'Content-Type': 'application/json',
  'User-Agent': 'myApplicationName/1.0',
  'Accept': 'application/json'
}
conn.request("POST", "/api/v2/statements", payload, headers)
res = conn.getresponse()
print(res.code)
data = res.read()
data1 = json.loads(data)

entries_to_remove = ('code', 'statementStatusUrl','requestId','sqlState','statementHandle','message','createdOn')
for k in entries_to_remove:
    data1.pop(k, None)



realdata = data1['data']
print(realdata)


column_names = data1['resultSetMetaData']['rowType']
# print(column_names)
column_names1 = column_names[0]['name']
print(column_names1)

column_names2 = column_names[1]['name']
print(column_names2)

Column_list = [column_names1,column_names2]
print(Column_list)

df = df = pd.DataFrame()


list_of_Customkey = []
for elem in realdata:
    
    list_of_Customkey.append(elem[0])

# print(list_of_Customkey)
list_of_Total= []
for elem in realdata:
    # print(elem[1])
    list_of_Total.append(elem[1])

# print(list_of_Total)

df[column_names1] = list_of_Customkey
df[column_names2] = list_of_Total


print(df)