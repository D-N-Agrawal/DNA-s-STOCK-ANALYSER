
''' 
     This CODE is an API of APACHE SPARK, written in PYTHON 
     It performs some analysis on DATA of 'NEWYORK STOCK EXCHANGE (NYSE)' of year 2009-2013
     It use RDDs and perfrms some of its transformations an actions to have some results

     To execute the API. its has to be submitted to spark using command < spark-submit DNA.py COMMAND >
     where COMMAND is an space saperated string of some subcommands which has to be in order
     The  format of all the commands are
     
     cross year < YEAR: YYYY > -CSA
     cross year < YEAR: YYYY > -CSA --mCCA
     cross year < YEAR: YYYY > -CSA --mCCA --mCMA
     cross year < YEAR: YYYY > -CSA --mCMA
     cross year < YEAR: YYYY > -CMA < company >

     cross dates < DATES: DD/MM/YY_to_DD/MM/YY > -CSA
     cross dates < DATES: DD/MM/YY_to_DD/MM/YY > -CSA --mCCA
     cross dates < DATES: DD/MM/YY_to_DD/MM/YY > -CSA --mCCA --mCMA
     cross dates < DATES: DD/MM/YY_to_DD/MM/YY > -CSA --mCMA
     cross dates < DATES: DD/MM/YY_to_DD/MM/YY > -CMA < company >
     


     ohlc year < YEAR: YYYY > < company >
     ohlc year < YEAR: YYYY > <month: Mon >< company >
     
     ohlc dates < DATES: DD/MM/YY_to_DD/MM/YY > < company >
     ohlc dates < DATES: DD/MM/YY_to_DD/MM/YY > <month: Mon >< company >

     EXAMPLES:
              1. cross year 2010 -CSA --mCCA --mCMA
              2. cross year 2011 -CMA BAC
              3. cross dates 3/10/09_to_30/9/10 -CSA --mCCA --mCMA
              4. ohlc dates 3/5/11_to_25/9/11 Feb A
              5. ohlc year 2013 BAC


     COMMAND INTERRETATION  :
            'cross' : cross is for comparative analysis of volumes within sectors or companies or months
                  CSA: Cross Sector Analysis, it aggregate total volume (within given time)of sector and plot a bar graph to show aggregation of each sector
                  CCA: Cross Company Analysis, it aggregate total volume (within given time)of company and plot a pie graph to show aggregation of each company
                  CMA: Cross Month Analysis, it aggregate total volume (within given time)of month of sectors or companies and plot a pie or bar graph to show aggregation of each months

                  m : if 'm' is leading any command, it means the command is to be performed on the max result of its previous command
                      as '-CSA --mCCA' says that perform cross sector and then perform cross company on the companies of max volume's sector in the result of cross sector

             'ohlc' : ohlc is for ploting (OPEN HIGH LOW CLOSE ) candle_stic graph of given time
                       1. if month is not given in command then it will plot ohlc for total given time otherwise only for the given month 
           
 
'''

#import context for SPARK:
try:
     from pyspark import SparkContext, SparkConf
except Exception as e:
     print('DNA says: ',e)
else:
     print("#"*70, "\n"*2, 'DEV says: PYSPARK imported ssucessfully', "\n"*2, "#"*70 )



#import required modules     
try:
     import helper
     import numpy as np
     import sys
     
except Exception as e:
     print('DNA says: ',e)
else:
     print("#"*70, "\n"*2, 'DEV says: reuired modules imported perfectly', "\n"*2, "#"*70 )




# THE MAIN FUNCTION:
def complex_main():
     global ss
     ss = sys.argv
     if len(ss) == 1:
          print("DNA says: NO command found")
          return None
     elif(ss[1] == 'cross'):
          complex_cross()
          return None

     elif(ss[1] == 'ohlc'):
          complex_ohlc()
          return None

     else:
          print("DNA says: NOT a valid command, first command should br cross OR ohlc")
          return None
          

# method to compile the subcommands of CROSS analysis (complex)     

def complex_cross():
     if len(ss) == 2:
          print("DNA says: NO cross command found")
          return None
     
     elif ss[2] == 'year':
          if(not helper.validateYEAR_fetchDATA(ss[3],sc)):
               return None

     elif ss[2] == 'dates':
          if(not helper.validateDATES_fetchDATA(ss[3],sc)):
               return None

     del(ss[1:4])
     
     helper.preprdd()
     
     if ss[1] == '-CSA':
          helper.CSA()
     elif ss[1] == '-CMA':
          if len(ss) < 3:
               print('DNA says: provide a company name to have CMA')
          elif(helper.isValidComp(ss[2])):
               helper.cCMA(ss[2])
               return None
          else:
               print('DNA says: ',ss[2], 'is not a company listed in NYSE')
               return None
     else:
          print('DNA says: ',ss[1], 'is not suppose to be first cmd')
          

     if len(ss) >= 3:
          if ss[2] == '--mCCA':
               helper.CCA()
          elif(ss[2] == '--mCMA'):
               helper.sCMA()
               return None
          else:
               print('DNA says: ',ss[2], 'is not suppose to be second cmd')

     if len(ss) == 4:
          if ss[3] == '--mCMA':
               helper.cCMA('')
          else:
               print('DNA says: ',ss[3], 'is not suppose to be THIRD cmd')
     return None







def complex_ohlc():
     if len(ss) == 2:
          print("DNA says: NO ohlc command found")
          return None
     
     elif ss[2] == 'year':
          if(not helper.validateYEAR_fetchDATA(ss[3],sc)):
               return None

     elif ss[2] == 'dates':
          if(not helper.validateDATES_fetchDATA(ss[3],sc)):
               return None

     del(ss[1:4])

     if len(ss) == 3:
          if(helper.isValidMonth(ss[1])):
               if(helper.isValidComp(ss[2])):
                    helper.ohlc(ss[1], ss[2])

               else:
                     print('DNA says: ',ss[2], 'is not a company listed in NYSE')
                    
          else:
               print('DNA says: ',ss[1], 'is not a month')
               return None
               
     elif len(ss) == 2:
          if(helper.isValidComp(ss[1])):
               helper.ohlc('',ss[1])
          else:
               print('DNA says: ',ss[2], 'is not a company listed in NYSE')
          





# setting up the SparkContext

print("#"*70, "\n"*2, 'DEV says: INFORMATION ABOUT SPARK CONTEXT ', "\n"*2)

conf = SparkConf().setAppName("DNA's STOCK ANALYSER")
sc = SparkContext(conf =conf)
sc.setLogLevel("ERROR")

print("\n"*2,"#"*70)


#calling main
complex_main()



