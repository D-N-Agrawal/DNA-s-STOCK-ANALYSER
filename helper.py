''' This module contains all the required methods '''






import re
import matplotlib.pyplot as plt
import mpl_finance
from matplotlib.dates import date2num
import datetime as dt


#Method to validate year and fetch data
def validateYEAR_fetchDATA(year,sc):
    ''''the method need two parameter :
       1. year : an year in format YYYY, methods return True if year is between 2009 - 2013
       2. sc : object of SparkContext to fetch data as an RDD
    '''   
    if(re.match('2009|201[0-3]',year)):
       path = r"C:/Users/HP/Desktop/spark_rel/DNAdb/nyse_" + year + ".csv"
       global data
       data = sc.textFile(path)
       data.cache()  #data RDD of given year, element like : ""
       global companies
       companies = sc.textFile("C:/Users/HP/Desktop/spark_rel/company_list.csv")
       companies.cache()  #companies inforfation RDD, element like: ""
       return True
    else:
        print("DNA says: ", year , "is not a valid year, the valid years are from 2009 to 2013")
        return False



#Method to validate dates and fetch data
def validateDATES_fetchDATA(dates,sc):
    '''the method need two parameter :
       1. dates : in format DD/MM/YY_to_DD/MM/YY, methods return True if dates are:
              1.not in valid format
              2.between year 2009 to 2013
              3.till date is greater then from date
              4.difference between dates is less then a year (365 days)

    
       2. sc : object of SparkContext to fetch data as an RDD
    '''  
    d1 = dates.split("_to_")[0]
    d2 = dates.split("_to_")[1]
    yd1 = d1.split('/')[2]
    yd2 = d2.split('/')[2]

    try:
        d1 = dt.datetime.strptime(d1,'%d/%m/%y')
        d2 = dt.datetime.strptime(d2,'%d/%m/%y')
    except Exception as e:
        print('DNA says: Not a valid date, ERROR is :' , e)
        return False


    if(d1>d2):
        print('DNA says: till_date is less then from_date')
        return False

    if((d1 < dt.datetime(2009,1,1)) or (d2 > dt.datetime(2013,12,31))):
        print('DNA says: only 01/01/2009 to 31/12/2013 data is available ')
        return False

    if(d2 - d1 > dt.timedelta(365)):
        print('''DNA says: for convinent plotting and ease of understanding of result
              dates should have diffrence of 365 days only ''')
        return False

    
    path1 = r"C:/Users/HP/Desktop/spark_rel/DNAdb/nyse_20" + yd1 + ".csv"
    path2 = r"C:/Users/HP/Desktop/spark_rel/DNAdb/nyse_20" + yd2 + ".csv"
    data1 = sc.textFile(path1)
    data2 = sc.textFile(path2)

    date_data1 = data1.map(lambda x: (convert_to_datetime(x.split(',',2)[1]) , x))
    date_data2 = data2.map(lambda x: (convert_to_datetime(x.split(',',2)[1]) , x))


    fil_data1 = date_data1.filter(lambda x: x[0] >= d1).map(lambda x: x[1])
    fil_data2 = date_data2.filter(lambda x: x[0] <= d2).map(lambda x: x[1])

    global data
    if(yd1 == yd2):
        data = fil_data1.intersection(fil_data2)
    else:
        data = fil_data1.union(fil_data2)

    data.cache() #data RDD of given year, element like : ""
    
    global companies
    companies = sc.textFile("C:/Users/HP/Desktop/spark_rel/company_list.csv")
    companies.cache() #companies inforfation RDD, element like: ""
    return True








# method to prepare some required RDDs
def preprdd():
    sdata = data.map(lambda x: (x.split(",")[0],x.split(",")[-1]))
    global fans
    fans = sdata.reduceByKey(lambda a,b : int(a)+int(b)) # fans RDD has all the companies as keys with the aggrigation of their daily volume
    fans.cache()    # elements like: ""
    
    sec_com = companies.map(lambda x: (x.split("|")[6] , x.split("|")[0]))
    global sec_coms
    sec_coms = sec_com.groupByKey()  #sec_coms RDD has all the sectors as keys and a list of their companies as value
    sec_coms.cache()  #elements like: ""
    return None





# dictonary of all 12 months with thier number, used to convert month with its number
mon_dict = {'Jan':1, 'Feb':2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6, 'Jul':7, 'Aug':8, 'Sep':9, 'Oct':10, 'Nov':11 , 'Dec':12}


#method to validate the month
def isValidMonth(mon):
    "this method accepts month name (Format : Mon) and return Ture if it is a valid month " 
    if mon in mon_dict.keys():
        return True
    else:
        return False
        


    
# method to get key of max value from dictonary
def key_o_max_val(d):
    "this method accpets a dictonary with values as integer and return the key of maximaam value in dictonary"
    v=list(d.values())
    k=list(d.keys())
    return k[v.index(max(v))]



# method to convert a date to float number 
def convertdt(sdate):
    "this method accepts an string date(Format: 'DD/Mon/YYYY') and return a float number eqv. to date"
    lst = sdate.split('-')
    fdate = dt.datetime( int(lst[2]) , mon_dict[lst[1]], int(lst[0]) )
    return date2num(fdate)


#method to convert a string date to datetime format 
def convert_to_datetime(sdate):
    "this method accepts an string date(Format: 'DD/Mon/YYYY') and return datetime eqv. to date"
    lst = sdate.split('-')
    fdate = dt.datetime( int(lst[2]) , mon_dict[lst[1]], int(lst[0]) )
    return fdate


#method to check weather company is listed with NYSE or not
def isValidComp(comp):
    "this method accepts the stock_ticker of company and return True if its in list of NYSE"
    company = companies.map(lambda x: x.split("|")[0]).collect()
    if comp in company:
        return True
    else:
        return False



     


       



# method for cross sector analysis (uses results of preprdd methods): 
def CSA():
     dfans = dict(fans.collect())
     global dic1
     dic1 = {}
     for i in sec_coms.collect():
         tot = 0
         for j in i[1]:
             try:
                 tot = tot + dfans.pop(j)
             except KeyError as e:
                 continue
         dic1[i[0]] = tot

     #VISUALIZAZION of CSA
     print("#"*70, "\n"*2, dic1, "\n"*2, "#"*70 )
     print(dic1)
     sectors = list(dic1.keys())
     volumes = list(dic1.values())
     barlist = plt.bar(sectors,volumes, align='center',alpha=0.5)
     barlist[volumes.index(max(volumes))].set_color("red")
     plt.xticks(rotation = 20, horizontalalignment="right")
     plt.ylabel('yearly volume')
     plt.xlabel('Industries')
     plt.title('sector stock analyse')
     plt.tight_layout()
     plt.legend(['VOLUME: 1 unit = 10000000000'])
     plt.show()
     return None





# method for cross company analysis (uses results of preprdd methods): 
def CCA():
     dfans = dict(fans.collect())
     global dic2
     dic2 = {}
     for i in sec_coms.collect():
         if i[0] == key_o_max_val(dic1):
             for j in i[1]:
                 try:
                     dic2[j] =  dfans.pop(j)
                 except KeyError as e:
                     continue
                    
     #VISUALIZAZION of CCA
     print("#"*70, "\n"*2, dic2, "\n"*2, "#"*70 )               
     print(dic2)
     companies = dic2.keys()
     cvolumes  = dic2.values()
     plt.pie(cvolumes,labels = companies,autopct = '%1.5f%%',shadow = True, startangle = 140)
     plt.axis('equal')
     plt.tight_layout()
     plt.show()
     return None



# method for cross MONTHS analysis of 'sector with max volume' found in cross sector analysis 
def sCMA():
     com_mon_vol = data.map(lambda x: ( x.split(",")[0],x.split(",")[1].split('-')[1],x.split(",")[-1]) )
     temp = companies.filter(lambda x: x.split('|')[6]  == key_o_max_val(dic1)).map(lambda x: x.split('|')[0])
     max_sec_coms = list(temp.collect())
     m_com_mon_vol = com_mon_vol.filter(lambda x : x[0] in max_sec_coms).map(lambda x : (x[1] , x[2]))
     fin_rdd = m_com_mon_vol.reduceByKey(lambda a,b : int(a) + int(b))
     dic5 = dict(fin_rdd.collect())
          
     #VISUALIZAZION of CMA
     print("#"*70, "\n"*2, dic5, "\n"*2, "#"*70 )
     print(dic5)
     months = dic5.keys()
     volumes  = dic5.values()
     plt.pie(volumes,labels = months,autopct = '%1.5f%%',shadow = True, startangle = 140)
     plt.axis('equal')
     plt.title(key_o_max_val(dic1))
     plt.show()
     return None



# method for cross MONTHS analysis of 'company with max volume' found in cross company analysis 
def cCMA(comp):
     if(comp == ''):
         dmax = key_o_max_val(dic2)
     else:
          dmax = comp
     sdata1 = data.map(lambda x: x.split(",",1))
     fdata = sdata1.filter(lambda x: x[0] == dmax)
     mdata = fdata.map(lambda x: (x[1].split(",")[0].split("-")[1] , x[1].split(",")[-1]))
     rdata = mdata.reduceByKey(lambda a,b : int(a) + int(b))    
     dic3 = dict(rdata.collect())

     #VISUALIZAZION of CMA
     print("#"*70, "\n"*2, dic3, "\n"*2, "#"*70 )
     print(dic3)
     
     months = list(dic3.keys())
     volumes =  list(dic3.values())
     barlist = plt.bar(months,volumes, align='center',alpha=0.5)
     barlist[volumes.index(max(volumes))].set_color("red")
     plt.ylabel('months')
     plt.xlabel('MONTHS')
     plt.title(dmax)
     plt.legend(['VOLUME: 1 unit = 100000000'])
     plt.show()
     return None



def ohlc(mon, com):
     sp_data = data.map(lambda x: x.split(','))
     
     if mon == '':
          fil_data = sp_data.filter(lambda x: x[0] == com)

     else:
          fil_data = sp_data.filter(lambda x: x[0] == com and x[1].split('-')[1] == mon)
          

     dates = fil_data.map(lambda x : x[1]).collect()
     map_fil_data = fil_data.map(lambda x: ( convertdt(x[1]) , float(x[2]), float(x[3]), float(x[4]), float(x[5]) ))
     quotes =  map_fil_data.collect()

     # #VISUALIZAZION of ohlc
     import matplotlib.ticker as ticker
     ax = plt.gca()
     ax.xaxis.set_major_locator(ticker.MaxNLocator(len(dates)))
     ax.set_xticklabels(dates)
     plt.xticks(rotation = 20, horizontalalignment="right")
     h = mpl_finance.candlestick_ohlc(ax, quotes)
     plt.show()


