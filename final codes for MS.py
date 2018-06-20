import pandas as pd
from urllib.request import urlopen
from bs4 import BeautifulSoup
import os         #import modules

# Crawl ChinaFund for open funds' NAV
url=r'http://data.chinafund.cn/'
#China Fund Web's net asset values for a specific date
urlString= urlopen(url)
soup= BeautifulSoup(urlString, 'html.parser')
nameList= soup.findAll('div',{'id':'content'}) #print(nameList)
for name in nameList:
    nameString= name.getText(',')  #get raw data
nameString= nameString.replace('--','0')
#'--' means NA on this website replace as '0' to easy the next steps like float()
nameString= nameString.splitlines() # split lines by '\r'(the default method)
#print(nameString)
data=[]  #data empty list
for line in nameString:
    lines= line.split(',')  #split text by ','
    data+=[lines[1:5]]  # the 2nd, 3rd, 4th, 5th data in each line
colnames=['Date','Symbol','Fundname','NAV'] # assign col names
dataFrame=pd.DataFrame(data,columns=colnames)
dataFrame=dataFrame[4:len(dataFrame)-2]  #delete the first 4 lines and the last 2 lines which are invalid
del dataFrame['Date']   #It's better for you to remain 'Date' and 'Fundname'
del dataFrame['Fundname']  #these 2 cols are useless for my job(maybe you need them)
##print(dataFrame)
dataFrame['Symbol']= dataFrame['Symbol'].astype(float)  #transfer NAV to float 
dataFrame['NAV']= dataFrame['NAV'].astype(float)
# transfer Symbol to float(I need to do that to remove first few zeroes for easy work)
writerfile= ('D:/MS/Allprice.csv')  #this step is for a combination later
#you don't need to do like me, maybe a function like dataFrame.to_excel(filePath) is better for you guys
dataFrame.to_csv(writerfile,encoding='utf_8_sig',index=False)

### Crawl ChinaFund for Money Funds' NAV
url=r'http://data.chinafund.cn/hb/' #URL of Money Funds' NAV
urlString= urlopen(url)
soup= BeautifulSoup(urlString, 'html.parser')
nameList= soup.findAll('div',{'id':'content'}) #print(nameList)
for name in nameList:
    nameString= name.getText(',')  #get raw data
nameString= nameString.replace('--','0')
#'--' means NA on this website replace as '0' to easy the next steps like float()
nameString= nameString.splitlines() # split lines by '\n'(the default method)
#print(nameString)
data=[]
for line in nameString:
    lines= line.split(',')  #split text by ','
    data+=[lines[1:4]]  # the 2nd, 3rd, 4th data in each line
colnames=['Symbol','Fundname','NAV']
dataFrame=pd.DataFrame(data,columns=colnames)
dataFrame=dataFrame[4:len(dataFrame)-2]
#delete the first 4 lines and the last 2 lines which are invalid
del dataFrame['Fundname'] #useless for my job , better for you guys to remain
dataFrame['Symbol']= dataFrame['Symbol'].astype(float)  #transfer NAV to float 
dataFrame['NAV']= dataFrame['NAV'].astype(float)
# transfer Symbol to float(I need to do that to remove first few zeroes for easy work)
writerfile=r'D:/MS/Money.csv'
dataFrame.to_csv(writerfile,encoding='utf_8_sig',index=False)

###Crawl TianTianFund for QDII Funds NAVs(eastfund website)
url= r'http://fund.eastmoney.com/QDII_jzzzl.html'
urlString= urlopen(url)
ttFund= BeautifulSoup(urlString, 'html.parser')
value=ttFund.find_all(name='td',attrs={'class':'dwjz black'}) # extract raw NAVs
Nav=[] # empty list for NAVS
n=0  #used as a conditinal selection
for nav in value:
    n+=1
    if n%2==0:
        Nav.append(float(nav.get_text().replace('---','0')))
    #odd every 2 value in variable 'value' would be the NAV I need
    #QDII'S NAV would be public experiencing a 2-day lag in China so what I need is T-2 NA
temp= ttFund.find_all(name='td',attrs={'colspan':2})
a=temp[1]  
date=[]
for i in a:
    date.append(i.get_text().strip())     
# codes above is for getting the date of these NAVs, you can use datetime.now or datedelta(), but this may need a conditonal logy for help because of weekends
NavDF=pd.DataFrame(Nav,columns=date) #dataframe Nav
symbol=[]
symbolList= ttFund.find_all(name='td', attrs={'class','bzdm'})
for ID in symbolList:
    symbol.append(float(ID.get_text()))  #get fund symbols
colnames=['Symbol']
symbolDF= pd.DataFrame(symbol, columns= colnames)
dataCombined= pd.concat([symbolDF,NavDF],axis=1) #combined two dataframe by cols
filePath=r'D:/MS/TT_QDII.csv'
dataCombined.to_csv(filePath, index=False)

### Crawl haomaiwang LICAI Funds
url= r'https://www.howbuy.com/board/licai.htm' #URL
urlString= urlopen(url)
soup= BeautifulSoup(urlString,'html.parser')
symbolList= soup.findAll('td',{'width':'6%'}) #raw symbols
symbol=[]
for ID in symbolList:
    symbol.append(float(ID.get_text()))  #get final cleaned symbols
priceList= soup.findAll('td',{'width':'9%'}) #raw price
Nav=[]
n=0
for price in priceList:
    n+=1
    if n%4==1:
        Nav.append(float(price.get_text(',').replace('--','0'))) #every four value is NAV
    else:
        pass  #in fact, you don't need to type 'else' codes, it's useless in thi case
symbolDF=pd.DataFrame(symbol,columns=['Symbol'])
NavDF= pd.DataFrame(Nav, columns=['Nav'])
temp= pd.concat([symbolDF,NavDF],axis=1) #combined symbols and NAVs by cols
filePath=r'D:/MS/licai.csv'    
temp.to_csv(filePath,encoding='utf_8_sig',index=False) 

### combined all csv files above

##add money funds' NAVs into Allprice.csv
filepath=r'D:\MS\Money.csv'  # ChinaFund open funds
newFile=r'D:\MS\Allprice.csv'  #New filepath for over-all funds
openFunds=open(filepath,'r',encoding='utf8')
data=openFunds.readline()  #read the first row which is not needed(first row is title)
data= openFunds.readline().replace('\n','') #replace '\n' with blanks
oldDictionary={}
while data !='':  #continue when dataline is not blank
    dataList= data.split(',') #because it is csv file
    symbol=dataList[0] #first value is symbol for every dataList
    nav= dataList[1]  #sencond value is NAV for every dataList
    oldDictionary[symbol]= nav+ '\n' #'\n' is for next row
    data= openFunds.readline().replace('\n','') # for next loop
openFunds.close()
temp= open(newFile,'a',encoding='utf8') #append newFile with oldDictionary
for i in oldDictionary:
    temp.write(i+',' +oldDictionary[i]) # i would be symbols
    
##Add ETF into Allprice.csv
url=r'http://fund.eastmoney.com/cnjy_jzzzl.html'
urlString= urlopen(url)
soup= BeautifulSoup(urlString,  'html.parser')
fundTable= soup.findAll('tr',{'height': '20'})
symbol=[]
price=[]  
n=0
for i in fundTable:
    td=i.find_all('td')
    for ID in td:
        n+=1
        if n==4:
            try:
                symbol.append(ID.getText(','))
            except ValueError:
                pass 
        if n==7:
            try:
                price.append(ID.getText(',').replace('---','0'))
            except ValueError:
                pass
    n=0
symbol=symbol[2:]
price=price[1:]
for i in range(len(symbol)):
    temp.write(symbol[i]+ ','+ price[i]+'\n')
temp.close()  #Money funds' NAVs have been added into Allprice.csv(which already has openFunds in it)
os.remove(filepath)

## combined haomai LICAI funds' NAVs into Allprice.csv
filePath=r'D:/MS/licai.csv'
temp= open(filePath,'r',encoding='utf8')
data=temp.readline()  #read the first row which is not needed(first row is title)
data= temp.readline().replace('\n','') #replace '\n' with blanks
oldDictionary={}
while data !='':  #continue when dataline is not blank
    dataList= data.split(',') #because it is csv file
    symbol=dataList[0] #first value is symbol for every dataList
    nav= dataList[1]  #sencond value is NAV for every dataList
    oldDictionary[symbol]= nav+ '\n' #'\n' is for next row
    data= temp.readline().replace('\n','') # for next loop
temp.close()
temp= open(newFile,'a',encoding='utf8') #append newFile with oldDictionary
for i in oldDictionary:
    temp.write(i+',' +oldDictionary[i]) # i would be symbols 
temp.close()  #LICAI funds' NAVs have been added into Allprice.csv(which already has openFunds in it)
os.remove(filePath)

#VLOOKUP AND IF in excel
import openpyxl
##T-1 fund NAVs
filePath= r'C:\Users\18455\Desktop\temp\1.csv'
temp=pd.read_csv(filePath)
temp['csrsPrice']= temp['csrsPrice'].fillna(0)
temp['ourkuPrice']= temp['ourkuPrice'].fillna(0)
temp['MSPrice']= temp['MSPrice'].fillna(0)
##print(temp)
a={}
symbol=temp['Symbol']  #extract symbols

n=0
for i in symbol:   
    a[i]=temp.ix[n]
    n+=1
filePath=r'D:\MS\Allprice.csv'
temp= pd.read_csv(filePath)
symbol= temp['Symbol']
n=0
b={}
for s in symbol:
    b[s]= temp.ix[n]
    n+=1
c=a.keys()
d=b.keys()
final=[]
na=[]
for i in c:
    if 'QDII' not in a[i][2]:
        if i in d:
            if b[i][1]==0:
                if a[i][7] !=0:
                    a[i][6]=a[i][7]
                    finalTuple=tuple(a[i][0:10])
                    final.append(finalTuple)
                elif a[i][8] !=0:
                    a[i][6]=a[i][8]  #IF function in excel
                    finalTuple=tuple(a[i][0:10])
                    final.append(finalTuple)
            else:
                a[i][6]=b[i][1]     #vlookup
                finalTuple=tuple(a[i][0:10])
                final.append(finalTuple)
        elif i not in d:
            if a[i][7]!=0 :
                a[i][6]=a[i][7]
                finalTuple=tuple(a[i][0:10])
                final.append(finalTuple)
            elif a[i][8]!=0 :
                a[i][6]=a[i][8]
                finalTuple=tuple(a[i][0:10])
                final.append(finalTuple)            
            else:    
                a[i][6]='NA'
                naTuple=tuple(a[i][0:10])
                na.append(naTuple)
col_name=['FundClassId','Symbol','DisplayName','CategoryName','InceptionDate','PriceDate','MSPrice','csrsPrice','ourkuPrice','PerformanceId']
finalFrame= pd.DataFrame(final,columns=col_name) #final frame for T-1 values
naFrame=pd.DataFrame(na,columns=col_name)
fileName=r'C:\Users\18455\Desktop\temp\price missing.xlsx' ###IMPORTANT
naFile=r'C:\Users\18455\Desktop\temp\na.csv'
fileName_nonqdii=r'C:\Users\18455\Desktop\temp\nonqdii.xlsx'
finalFrame.to_excel(fileName_nonqdii,index=False)### nonqdii file
writer= pd.ExcelWriter(fileName,engine='openpyxl')###IMPORTANT
finalFrame.to_excel(writer,sheet_name='nonqdii',index=False)###IMPORTANT
naFrame.to_csv(naFile,encoding='utf_8_sig',index=False)

##T-2 fund NAV for QDII, same as above
filePath= r'C:\Users\18455\Desktop\temp\2.csv' #must be named as 2 and in csv format
temp=pd.read_csv(filePath)
temp['csrsPrice']= temp['csrsPrice'].fillna(0)
temp['ourkuPrice']= temp['ourkuPrice'].fillna(0)
temp['MSPrice']= temp['MSPrice'].fillna(0)
##print(temp)
a={}
symbol=temp['Symbol']  #extract symbols
n=0
for i in symbol:   
    a[i]=temp.ix[n]  #nth line in x
    n+=1
filePath=r'D:\MS\TT_QDII.csv'
temp= pd.read_csv(filePath)
symbol= temp.iloc[:,0]  #extract symbol from the 1st column of temp
n=0
b={}
for s in symbol:
    b[s]= temp.ix[n]
    n+=1
c=a.keys()
d=b.keys()
final=[]
na=[]
for i in c:   #method is same as above
    if 'QDII' in a[i][2]:
        if i in d:
            if b[i][1]==0:
                if a[i][7] !=0:
                    a[i][6]=a[i][7]
                    finalTuple=tuple(a[i][0:10])
                    final.append(finalTuple)
                elif a[i][8] !=0:
                    a[i][6]=a[i][8]  #IF function in excel
                    finalTuple=tuple(a[i][0:10])
                    final.append(finalTuple)
            else:
                a[i][6]=b[i][1]     #vlookup
                finalTuple=tuple(a[i][0:10])
                final.append(finalTuple)
        elif i not in d:
            
            if a[i][7]!=0 :
                a[i][6]=a[i][7]
                finalTuple=tuple(a[i][0:10])
                final.append(finalTuple)
            elif a[i][8]!=0 :
                a[i][6]=a[i][8]
                finalTuple=tuple(a[i][0:10])
                final.append(finalTuple)            
            else:    
                a[i][6]='NA'
                naTuple=tuple(a[i][0:10])
                na.append(naTuple)        
col_name=['FundClassId','Symbol','DisplayName','CategoryName','InceptionDate','PriceDate','MSPrice','csrsPrice','ourkuPrice','PerformanceId']
finalFrame_qdii= pd.DataFrame(final,columns=col_name)
naFrame=pd.DataFrame(na,columns=col_name)
##fileName=r'C:\Users\18455\Desktop\temp\price missing.csv' has been assigned before
finalFrame_qdii.to_excel(writer,sheet_name='qdii',index= False)
writer.save()
writer.close()# close here not codes above because I need to add a sheet into an existing file
fileName_nonqdii=r'C:\Users\18455\Desktop\temp\qdii.xlsx'
finalFrame_qdii.to_excel(fileName_nonqdii,index=False)### nonqdii file
naFile=r'C:\Users\18455\Desktop\temp\na_qdii.csv'
##finalFrame.to_csv(fileName,encoding='utf_8_sig',index=False)
naFrame.to_csv(naFile,encoding='utf_8_sig',index=False)
final=pd.concat([finalFrame,finalFrame_qdii],axis=0,ignore_index=True)
filePath=r'C:\Users\18455\Desktop\temp\BatchCNDailyPrice.csv' #IMPORTANT!!this file is for upload
col_name=['PerformanceId','PriceDate','ourkuPrice']
final['ourkuPrice']=final['MSPrice'] #MS codes are wrongly coded, I have to follow them
final.to_csv(filePath,encoding='utf_8_sig',index=False,columns=col_name)
