# DNA-s-STOCK-ANALYSER
Its my very first work i'm adding on git



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
           

