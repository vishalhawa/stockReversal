
# ---Source for all useful Finance Utilities ------

# startDate=(as.Date("01/01/2008",format="%m/%d/%Y"))
# endDate = as.Date("12/31/2009",format="%m/%d/%Y")
# getStockHist("AAPL",startDate,endDate)


library(quantmod) %>% suppressMessages
library(fOptions)
library(jsonlite)
library(data.table) %>% suppressMessages
library(ggplot2)
library(tidyverse)

library(XML)
library(BatchGetSymbols) %>% suppressMessages
library(parallel)

library(magrittr)
library(httr)


## Util Functions ------------>
processNumber<-function(x){
  x=str_replace_all(x,",","")
  x=str_replace_all(x,"%","/100") # make to fractions
  x=str_replace_all(x,"N/A","0")
  x=str_replace_all(x,"K","*1000")
  x=str_replace_all(x,"M","*1000000")
  x=str_replace_all(x,"B","*1000000000")
  x=str_replace_all(x,"T","*1000000000000")
  x=tryCatch( eval(parse(text=x)), error= function(e) NA_integer_) 
  return(x)
}
vprocessNumber <- Vectorize(processNumber)

## Tickers / Quotes =====================================================

getAllTickers <- function(symbols,period='daily',thresh=0.5,parallel=T,days = 400){
  start = Sys.Date()-days
  keepcols = c( 'ref.date','price.open', 'price.high', 'price.low', 'price.close'  , 'volume'  ,'ticker','ret.closing.prices')
  options(future.rng.onMisue = "ignore")
  if(parallel) future::plan(future::multisession, workers = floor(parallel::detectCores()-2))
  quotes =  BatchGetSymbols(tickers = symbols,  first.date =  start,last.date = Sys.Date(),be.quiet=T,
                            do.cache=F,freq.data = period,thresh.bad.data = thresh,bench.ticker='SPY',do.parallel = parallel)
  quotes = na.omit(as.data.table(quotes$df.tickers)[, ..keepcols] )
  colnames(quotes)  <- c('index' , 'Open' , 'High' ,  'Low' ,'ClosePrice',   'Volume' ,'symbol','returns')
  
  setkey(quotes,'index')
  quotes
}
# getAllTickers('UBER',parallel=T)

getFirmClass_Tr <- function(tiks){  ##  This gets Firm Codes etc
  tik_spl =  split(unique(tiks), ceiling(seq_along(unique(tiks))/20))    ## payload restricted to 20 tiks quotes  

  res =   map_df(tik_spl, function(spl) {
    clss =   content(GET(url= paste0('https://api.tradier.com/beta/markets/fundamentals/company?symbols=',spl %>% str_c(collapse  = ',') ), 
                         add_headers( Authorization='Bearer HUvSnb0uwwcBfszEGC0rWKRVqOQb',  Accept='application/json' )))

      clss = data.table( req=map(clss, function(x) x %>% pluck('request'))  ,
                         rsl = map(clss, function(x) x %>% pluck('results'))  ) %>% drop_na() #compact ## remove error symbs
               pmap_df(clss,
                      function(req,rsl) {
                        dt = unlist(rsl) %>% enframe() %>% 
                          dplyr::filter(str_detect(name,'^tables.asset_classification.*')) %>% as.data.table()
                        dt= cbind(symbol=req,dt); dt
                      })
    
  })
  res$name =  res$name  %>% str_remove('tables.asset_classification.') 
  pivot_wider(res,symbol)
}
# getFirmClass_Tr(c('CNC','KSS','MSFT','CRAP','SE','CRWD')) 

# tp = getFirmClass_Tr(tickers[!is.na(IV_flag)]$Symbol)

getQuotes_Tr <- function(tiks){
  tiks = na.omit(tiks) %>% unique() %>% str_c(collapse  = ',') 
  quotes =  
    content(POST(url='https://api.tradier.com/v1/markets/quotes', 
                 body  = paste0('symbols=',tiks),
                 add_headers( Authorization='Bearer HUvSnb0uwwcBfszEGC0rWKRVqOQb', Accept='application/json',`Content-Type`='application/x-www-form-urlencoded'  )
    ))$quotes$quote  %>%  rbindlist(fill = T) %>% 
    transmute(symbol,index=as.Date(as.POSIXct(trade_date/1000, origin="1970-01-01")),ClosePrice=last,open , high ,  low ,close,Volume=volume,average_volume,returns=change_percentage/100 )
   # select(symbol,exch,last,volume,average_volume,change_percentage)
  quotes
}
# getQuotes_Tr(stocks$symbol)

## Fundamentals/Statistics -----------------------------------------------------------------------------------------------------

# curl -X GET "https://api.tradier.com/beta/markets/fundamentals/ratios?symbols=MSFT"      -H 'Authorization: Bearer HUvSnb0uwwcBfszEGC0rWKRVqOQb'      -H 'Accept: application/json'
# old  %>% htmlTreeParse( useInternalNodes = TRUE, asText = TRUE) %>%  getNodeSet( "//valuation_ratios") %>% .[[1]] %>% xml2::as_list()


getStats_Tr <- function(tiks, what=c('p_e_ratio','forward_p_e_ratio','p_e_g_ratio','p_s_ratio','p_b_ratio',
                                     'year_estimated_e_p_s_growth','as_of_date')){  ##  This gets valuation ratios for now.
  tiks = na.omit(tiks) %>% unique() %>% str_c(collapse  = ',') 
  tiks = paste0('AAPL,',tiks)
  stats =   content(GET(url= paste0('https://api.tradier.com/beta/markets/fundamentals/ratios?symbols=',tiks), 
                 add_headers( Authorization='Bearer HUvSnb0uwwcBfszEGC0rWKRVqOQb',  Accept='application/json' )))
  # {stats ->> tp}
  stats = data.table( req=map(stats, function(x) x %>% pluck('request'))  ,
                     rsl = map(stats, function(x) x %>% pluck('results'))  ) %>% drop_na()
  
  stats= pmap_df(stats,
          function(req,rsl) {
            dt =  unlist(rsl) %>% enframe() %>%  
              dplyr::filter(str_detect(name,'^tables.valuation_ratios.*')) %>% as.data.table() 
            dt= cbind(symbol=req,dt); dt
          })
  
  stats = stats %>% dplyr::filter(str_detect (name,paste0(what,collapse = '|')) ) %>% 
            mutate(name = str_remove(name,'tables.valuation_ratios.'))
  
  stats = stats[name %in% what] %>% 
          pivot_wider(symbol) %>% 
          type_convert() %>% suppressMessages %>% 
       #   mutate(earn_growth = p_e_ratio/forward_p_e_ratio) %>% 
          mutate(across(where(is.numeric), round,2)) %>% 
          transmute(symbol,pb=p_b_ratio,ps=p_s_ratio,pe=p_e_ratio,peg=p_e_g_ratio,pe_f=forward_p_e_ratio,eg=p_e_ratio/forward_p_e_ratio)  
    if(nrow(stats)>1) stats %>% dplyr::slice(-1) ## remove AAPL
      else stats
}  
# tp = getStats_Tr( tickers[!is.na(IV_flag)]$Symbol)
# getStats_Tr( tickers[!is.na(IV_flag)][2:2]$Symbol)
 
 ## OLD>>

 # xml2::xml_find_all(tp,"//p_e_ratio") %>% xml2::xml_double() 
 # xml2::xml_find_all(tp,"//p_e_ratio") %>% xml2::xml_name() 
 # xml2::xml_attr(tp,attr = "//p_e_ratio") 
 # 
 # xml2::as_list(tp) %>% as_tibble() %>% unnest_wider(items) %>% unnest(cols = names(.)) %>% unnest(cols = names(.))
 # 

# rbindlist( tp ) %>% unnest_wider(results, names_repair = 'unique') %>% 
#   unnest_wider(tables, names_repair = 'unique') %>% 
#   unlist('alpha_beta')

getFundStats <- function(stk){ ## Y! profile  - net Assets / category 
  stk = toupper(stk) ;   print(stk)   
  url <-  paste0('https://finance.yahoo.com/quote/',stk,'/profile?p=',stk)  
  webpage <- tryCatch(read_html(url),error=function(e) {print(e) ;return( NA)})
  keys = c('(Category)|(Fund Family)|(Net Assets)|(YTD Return)|(Yield)|(Morningstar Rating)|(Inception Date)')
  fnd  =    tryCatch( webpage %>% 
              html_nodes(".Mb\\(25px\\)") %>% .[[1]]  %>%
              html_text(),
              error=function(e) {print(e) ; return(list(fnd=stk))})
        
 if( !str_starts(fnd,'Fund') ) return(list(fnd=stk)) ## if Y! profile page does not exsists
 if( length(str_split(fnd ,keys,simplify = T) ) !=8) return(list(fnd=stk)) ## if Y! profile/Fund overview page malformed
  dt =  data.table(vprocessNumber(str_split(fnd ,keys,simplify = T)[,4:6]) ) %>% transpose() %>% 
           cbind( start_date = suppressWarnings(parse_date(str_split(fnd,keys) %>% unlist(),format = '%b %d, %Y')) %>% na.omit()   ) %>% 
           cbind(parse_character(str_split(fnd ,keys,simplify = T))[2]) %>% 
           cbind(parse_character(str_split(fnd ,keys,simplify = T))[3]) %>% 
            cbind( fnd = stk)
 setnames(dt,c('assets','returns_ytd','yield','start_date','category','family','fnd'))
 dt
}
 # map_dfr( c('VIGIX','AEYXX','VPMAX','CRAP','AADR') ,getFundStats) 

getETFStats <- function(stk){ ## Y! profile  - net Assets / category 
  stk = toupper(stk) ;   print(stk)   
  url <-  paste0('https://finance.yahoo.com/quote/',stk,'/profile?p=',stk)  
  webpage <- tryCatch(read_html(url),error=function(e) {print(e) ;return( NA)})
  keys = c('(Category)|(Fund Family)|(Net Assets)|(YTD Daily Total Return)|(Yield)|(Legal Type)')
  fnd  =    tryCatch( webpage %>% 
                        html_nodes(".Mb\\(25px\\)") %>% .[[1]]  %>%
                        html_text(),
                      error=function(e) {print(e) ; return(list(fnd=stk))})
  fnd = str_replace(fnd,' Yield ','Y_')
  if( !str_starts(fnd,'Fund') ) return(list(fnd=stk)) ## if Y! profile page does not exsists
  if( length(str_split(fnd ,keys,simplify = T) ) !=7) return(list(fnd=stk)) ## if Y! profile/Fund overview page malformed
  dt =  data.table(vprocessNumber(str_split(fnd ,keys,simplify = T)[,4:6]) ) %>% data.table::transpose() %>% 
    # cbind( start_date = suppressWarnings(parse_date(str_split(fnd,keys) %>% unlist(),format = '%b %d, %Y')) %>% na.omit()   ) %>% 
    cbind(parse_character(str_split(fnd ,keys,simplify = T))[2]) %>% 
    cbind(parse_character(str_split(fnd ,keys,simplify = T))[3]) %>% 
    cbind( fnd = stk)
  setnames(dt,c('assets','returns_ytd','yield','category','family','fnd'))
  dt
}
 # map_dfr( c('SPY','HYG','CRAP','FILL') ,getETFStats) 

getStockStats <- function(stk){ ## Finviz data 
  stk = toupper(stk) ;   print(stk)
  url <- paste0("http://finviz.com/quote.ashx?t=", stk)
  webpage <- tryCatch(readLines(url),error=function(e) {print(e) ;return( NA)})
  if(is.na(webpage) ) return(list(symbol=stk))
  html <- htmlTreeParse(webpage, useInternalNodes = TRUE, asText = TRUE)
  tableNodes <- getNodeSet(html, "//table")
  dt = data.table(readHTMLTable(tableNodes[[8]], header= c("data1", "data2", "data3", "data4", "data5", "data6",
                                                           "data7", "data8", "data9", "data10", "data11", "data12")))
  dt = rbind(dt[,.(data1,data2)],dt[,.(data3,data4)],dt[,.(data5,data6)],dt[,.(data7,data8)],dt[,.(data9,data10)],dt[,.(data11,data12)],use.names=F )
  dt = cbind(stk,dt)
  colnames(dt)<- c('symbol','key','value')
  dt = dcast(dt,symbol~key,value.var = 'value',fun=I,fill = NA)
  setnames(dt,'Target Price','targetPrice')
  setnames(dt,'Market Cap','mcap')
  dt$MedTarget = as.numeric(as.character(dt$targetPrice))
  dt$mcap =  processNumber(dt$mcap)
  dt$ROA =  processNumber(dt$ROA)
  dt$ROE =  processNumber(dt$ROE)
  dt$OPM =  processNumber(dt$`Oper. Margin`)
  dt$NPM =  processNumber(dt$`Profit Margin`)
  dt$DebtRatio =  processNumber(dt$`Debt/Eq`)
  dt$earnDate = as.Date(str_replace(dt$Earnings,'AMC|BMO','') , format='%b %d')
  dt$earnDate = ifelse(dt$earnDate < Sys.Date(),NA,dt$earnDate) %>% as.Date()  ## since year is missing trying to guess the year
  dt$earnTime = str_extract(dt$Earnings,'AMC|BMO')
  dt$Beta = processNumber(dt$Beta)
  dt$ATR = processNumber(dt$ATR)
dt
}
 # temp = lapply(c('SWKS','AA'), function(st) getStockStats(st)) %>% rbindlist(fill=T)

getStockData = function(stks,  hist_days = 100,  capm_days = 4 ){  ## Pull from Y! vec of stks-symbols
   stks = stocks[symbol %in%  stks] #   the Filter ..on stocks
  quotes_hist  =  getAllTickers(c('SPY',stks$symbol),thresh = 0.5,days = hist_days, parallel = nrow(stks)>20) %>% suppressWarnings  ## just above a quarter 
  print('Todays ..')
  # today_price = rbindlist(mclapply(stks$symbol %>% unique, 
  #                                  function(x) tryCatch(data.table(getQuote(x),symbol=x),error=function(e) data.table(symbol=x)) , mc.cores = parallel::detectCores()-2 ),   fill = T )
  # today_price = today_price[,.(symbol,index=as.Date(`Trade Time`),ClosePrice=Last,Open,High,Low,Volume,returns=`% Change`/100)]
  today_price = getQuotes_Tr(stocks$symbol)[,.(symbol, index, ClosePrice, Open=open,High=high, Low=low , Volume, returns)]
  quotes_hist = rbind(quotes_hist,today_price)
  quotes_hist = quotes_hist[symbol %in% quotes_hist[,.N,symbol][N>hist_days/2]$symbol] ## Filter dead symbols/ 
  quotes_hist = quotes_hist[!duplicated(quotes_hist[,.(index,symbol)])] ## de-dups 
  quotes_hist  = quotes_hist[,na.omit(.SD),symbol]
  quotes_hist[,vlty5 := sd(returns[(.N-4):(.N-0)],na.rm = T),symbol ] ## 1d-lag avoid data snooping not done
  quotes_hist[,vlty := sd(returns,na.rm = T) , symbol ] ## 
  quotes_hist[,vrank := vlty5/vlty ,symbol ] ## 
  quotes_hist[,rsi3 := round(RSI(ClosePrice,n=3)), symbol ] 
  quotes_hist[,ma3 := round(SMA(ClosePrice,3),2),symbol] 
  quotes_hist[,ma9_sl := round(1000*ROC(SMA(ClosePrice,9),type ='discrete'),2),symbol] 
  quotes_hist[,ma3_sl := round(1000*ROC(SMA(ClosePrice,3),type ='discrete'),2),symbol] 
  quotes_hist[,ma20 := round(SMA(ClosePrice,20),2),symbol] 
  quotes_hist[index > Sys.Date()-5, tail_mv := tail(.SD[abs(returns)> 2*vlty],1)$index, symbol]
  {quotes_hist ->> quotes_hist}
  print('Quotes over ..')
  support = quotes_hist[ index > Sys.Date()-10,{idx = findValleys(ma3) ; .SD[last(idx)-1]},symbol,.SDcols=c('index','ClosePrice','Open')] %>% ## Valley points last 10d
         drop_na() %>%  
         transmute(symbol,index ,support = pmin(ClosePrice,Open))
  ted_stk = quotes_hist[ symbol %in% stks[(actionDate-Sys.Date()) %between% c(1,9)]$symbol ,
                         {idx = findValleys(ma3) ; .SD[last(idx)-1]},symbol][index>Sys.Date()-12,.(symbol,idx_valley=index)]#[order(-returns)]$symbol
  
  
  stks = today_price[stks,on='symbol'][,.(symbol,actionDate, ClosePrice)] # attache Close Price 
  stks  = merge(stks,quotes_hist[,.(symbol,returns ,vrank,vlty,vlty5,ma9_sl,ma3_sl,tail_mv)],by='symbol',all.x=T)
  stks = stks[,.SD[.N],symbol] ## The latest record 
  stks  = merge(stks,support[,.(symbol,support)],by='symbol',all.x=T) ## << support Price
  stks  = merge(stks,ted_stk[,.(symbol,idx_valley)],by='symbol',all.x=T) ## << Valley Point
  
  print('CAPM ..takes a min..')
  capm_days = 5
  stks =  quotes_hist %>% group_by(symbol) %>% arrange(index) %>% dplyr::filter(index > Sys.Date()-capm_days ) %>% 
    select(symbol,index,returns) %>%
    left_join( "SPY" %>%  ## the benchmark
                 tidyquant::tq_get( from = Sys.Date()-capm_days-1, to   = Sys.Date()) %>%
                 tidyquant::tq_transmute(select = adjusted, mutate_fun = periodReturn,  period = "daily", col_rename = "spy_returns"),
               by = c('index'="date") ) %>% 
    tidyquant::tq_performance(Ra = returns,  Rb = spy_returns, performance_fun = ActivePremium) %>% 
    rename('active_premium' = 'ActivePremium.1')  %>% 
    select(symbol,active_premium) %>% as.data.table() %>% 
    .[stks,on='symbol'] # attache  AP, 
  print('CAPM .Over.')
  
  stks %>%    mutate(across(where(is.numeric),round,2))
}
# sdata = getStockData(stks = c('OKTA','NTNX','COIN'))
# getStockData(stks = stocks[1:25]$symbol )

## Earnings -----------------------------------------------------------------------------------------------------

getEarningsTipRanks <- function(from, to){  ## from Tip Ranks by dates..TBD
  if(from>to) { return(NULL) }
  # https://www.tipranks.com/calendars/earnings/2021-11-15 - another one 
  days = seq(as.Date(from), as.Date(to),by='day')
  earn = lapply(days , function(d) {     print(d) 
    url = paste0('https://www.fxempire.com/tools/earnings-calendar?date-from=',d,'&date-to=',d,'&marketcap=1%2C2%2C3')
    tableNodes = readLines(url,warn = F) %>% 
      htmlTreeParse( useInternalNodes = TRUE, asText = TRUE) %>% 
      getNodeSet( "//table")
    
    map(tableNodes , ~readHTMLTable(.x) ) %>% rbindlist %>%
      janitor::clean_names()  %>% 
      mutate(earnDate = d) 
  })
  rbindlist(earn,fill = T) %>% 
    mutate( due = parse_number(due)) %>%     
    mutate( due = if_else(due==24,0,due)) %>% ## 24 hrs -> 00
    mutate( due = due-5) %>%    ## get UST 
    mutate(actionDate = if_else(due>15,earnDate+1,earnDate)) %>% 
    select(-due,-earnDate)
}

getEarnings_Tr <- function(tiks){  ## TBD..
  tiks = na.omit(tiks) %>% unique() %>% str_c(collapse  = ',') 
  earn =  
    content(GET(url= paste0('https://api.tradier.com/beta/markets/fundamentals/calendars?symbols=',tiks), 
                add_headers( Authorization='Bearer HUvSnb0uwwcBfszEGC0rWKRVqOQb',  Accept='application/json' )
    ))
  # curl -X GET "https://api.tradier.com/beta/markets/fundamentals/ratios?symbols=MSFT"      -H 'Authorization: Bearer HUvSnb0uwwcBfszEGC0rWKRVqOQb'      -H 'Accept: application/json'
 temp =  rbindlist( tp ) %>% unnest_auto(results) %>% unnest_longer(tables) %>% 
   unnest_longer(tables) %>% unnest_longer(tables,names_repair = 'unique') %>% 
   mutate(tables = unlist(tables) ) %>% 
   dplyr::filter(tables_id...6 == 'begin_date_time') %>% 
   mutate(tables = parse_date(tables) ) %>% 
   dplyr::filter(tables > Sys.Date()) %>% 
   group_by(request)  %>% 
   dplyr::filter(tables==min(tables))  %>% 
   transmute(symbol=request,earnDate=tables) %>% 
   distinct() 

  earn
}
# tp =  getEarnings_Tr(c('AZO','F','TSLA'))

getEarningsFXempire <- function(from, to){  ## from FXempire by dates
  if(from>to) { return(NULL) }
  days = seq(as.Date(from), as.Date(to),by='day')
  earn = lapply(days , function(d) {     print(d) 
            url = paste0('https://www.fxempire.com/tools/earnings-calendar?date-from=',d,'&date-to=',d,'&marketcap=1%2C2%2C3')
            tableNodes = readLines(url,warn = F) %>% 
              htmlTreeParse( useInternalNodes = TRUE, asText = TRUE) %>% 
              getNodeSet( "//table")
            
               map(tableNodes , ~readHTMLTable(.x) ) %>% rbindlist %>%
              janitor::clean_names()  %>% 
               mutate(earnDate = d) 
    })
  rbindlist(earn,fill = T) %>% 
  # mutate( due = parse_number(due)) %>%     
  # mutate( due = if_else(due==24,0,due)) %>% ## 24 hrs -> 00
  # mutate( due = due-5) %>%    ## get UST 
  # mutate(actionDate = if_else(due>15,earnDate+1,earnDate)) %>% 
  mutate(actionDate = if_else(earnings_call_time>'After market close',earnDate+1,earnDate)) %>% 
  mutate( ticker = str_extract(company,"(?<=\\().+?(?=\\))"))  %>% 
  select(-earnings_call_time,-earnDate,-v1) %>% drop_na(ticker)
}
#  getEarningsFXempire(from=Sys.Date(),to=Sys.Date()+2)

getEarnings <- function(from,to) {  ## this from earnings calendar api
  if(from>to) { return(NULL) }
  days = gsub('-','',seq(as.Date(from), as.Date(to),by='day'))
  earn = lapply(days , function(d) {    Sys.sleep(1)
    url = paste0("https://freeapi.earningscalendar.net/?date=", d)
    print(url)
    earn =  as.data.table(fromJSON(url))
    if (nrow(earn) > 0) { #print(earn)
      earn[, .(symbol = ticker, earnDate = as.Date(d, format = '%Y%m%d'), earnTime = toupper( when))]
    } else {
      data.table( earnDate = as.Date(d, format = '%Y%m%d'))
    }
  })
  earn = rbindlist(earn,fill = T)
  #  earn[, cat :=cut(earn$cap_mm,c(0,500,2000,6000,15000,Inf),labels = c('tiny','small','mid','large','mega'))]
  # earn$earnDate =  earn$earnDate -1 ## this is due to bug in api : only 2017 reportings ?
  earn$actionDate = as.Date(ifelse(earn$earnTime=='AMC',earn$earnDate+1,earn$earnDate),origin = '1970-01-01')  ## adjust for when=amc
  return(earn)
}
# getEarnings(Sys.Date(),Sys.Date()+3) 

getEarningsHist <- function(from,to){  ## from Y! by dates
  if(from>to) { return(NULL) }
days = seq(as.Date(from), as.Date(to),by='day')
earn = lapply(days , function(d) {     print(d) 
  off=0 ## offset for innner loop 
  tab = data.table(Symbol = NA, actionDate=Sys.Date())
  while ( TRUE) {
    print(off)
    url = paste0('https://finance.yahoo.com/calendar/earnings?day=',d,'&offset=',off)
    tableNodes=     webpage <- readLines(url,warn = F) %>% 
      htmlTreeParse( useInternalNodes = TRUE, asText = TRUE) %>% 
      getNodeSet( "//table")
    if(length(tableNodes)<1) break
    tab= rbind(tab,data.table(readHTMLTable(tableNodes[[1]])) ,fill=T  )
    tab$`Earnings Call Time` = gsub('(Before Market Open)|(Time Not Supplied)|(TAS)','1:00AM EST',tab$`Earnings Call Time`)
    tab$`Earnings Call Time` = gsub('After Market Close','5:00PM EST',tab$`Earnings Call Time`)
    tab$actionDate = as.Date(ifelse(lubridate::hour(strptime(tab$`Earnings Call Time`,format="%I:%M%p"))>16,{as.Date(d)+1},{as.Date(d)})  ) 
    
    off=off+100
  }
  tab=tab[,.(ticker=as.character(Symbol),actionDate)] %>% na.omit()
  tab
  }) ## lapply 
earn = rbindlist(earn,fill = T) 
earn[,.(ticker,actionDate)] %>% na.omit()
}
# earnings =  getEarningsHist(from=Sys.Date(),to=Sys.Date()+2)

getEarningMoves <- function(tiks, days=800){
  map(tiks, function(tik){
    tik_hist=  suppressWarnings(tidyquant::tq_get( tik,from = Sys.Date()-days, to= Sys.Date()) ) %>% as.data.table() %>% 
      .[,.(symbol,date,close,move3=frollapply(close,3,function(x){round(-1+x[3]/x[1],2)},align = 'center'))]
    merge(tik_hist, 
          rbind(getEarningsHist2(symbol = tik)[earnDate>Sys.Date()-days,.(symbol,earnDate)],
                list(symbol= tik, earnDate=Sys.Date()-1)),
          by.x=c('symbol','date'),by.y=c('symbol','earnDate') )
  }) %>% rbindlist() %>% 
    drop_na
}

 # getEarningMoves(c('JPM','ABT'))  %>% .[,.(mv= mean(abs(gain3),na.rm=T) %>% round(2)), symbol] 


getEarningsHist2 <- function(symbol='FB'){  ## This is yahoo earnings BY Symbol
  print(symbol)
  url <- paste0("https://finance.yahoo.com/calendar/earnings?symbol=", symbol)
  webpage <- readLines(url,warn = F)
  html <- htmlTreeParse(webpage, useInternalNodes = TRUE, asText = TRUE)
  tableNodes <- getNodeSet(html, "//table")
  tab = data.table(Symbol = NA)
  
   if(length(tableNodes)>0){ 
    tab = data.table(readHTMLTable(tableNodes[[1]]))
    # tab$V1 = NULL
    # tab$`Earnings Call Time` = gsub('(Before Market Open)|(Time Not Supplied)','1:00AM EST',tab$`Earnings Call Time`)
    # tab$`Earnings Call Time` = gsub('After Market Close','5:00PM EST',tab$`Earnings Call Time`)
    # tab$actionDate = as.Date(ifelse(lubridate::hour(strptime(tab$`Earnings Call Time`,format="%I:%M%p"))>16,{as.Date(day)+1},{as.Date(day)})  )
  }
 tab[,earnDate := as.Date(lubridate::parse_date_time(str_extract(`Earnings Date`,'^.*[0-9]{4}'),'b d, Y'))]
 setnames(tab,c('Symbol','EPS Estimate','Reported EPS'),c('symbol','epsEst','epsActual'))[]
}
# getEarningsHist2(symbol = 'MRVL')

## Options ============================================================================
fetchOptionsChain<-function(date,stk) { ## through webb query - called by getOptions
  document <- tryCatch(fromJSON(txt=paste0('https://query2.finance.yahoo.com/v7/finance/options/',stk,'?date=',date),flatten = T) )
                       # error = function(e){data.table(inTheMoney=NA,OS=NA,OI=NA)} )
  calls = as.data.table(as.data.frame(document$optionChain$result$options)[,"calls"])
  puts = as.data.table(as.data.frame(document$optionChain$result$options)[,"puts"])
  # print(calls); print(puts)
  if(nrow(calls)<1 | nrow(puts)<1) { return(  data.table(contractSymbol=NA,openInterest=NA ,strike=NA,volume=NA,type=NA ,bid=NA,ask=NA,expiry = as.Date(NA),impliedVolatility= NA)) }
  calls$currency = "C"
  puts$currency = "P"
  options = rbind(calls,puts,fill=T)
  options$expiry  = as.Date(options$expiration/86400,origin = "1970-01-01")
  options$lastTradeDate  = as.Date(options$lastTradeDate/86400,origin = "1970-01-01")
  options$type=options$currency 
  options$currency = NULL
  return(as.data.table(options))
}
# fetchOptionsChain(date=dates[[1]][2],stk='DAC')

getOptions<-function(symbol){ ## through Web query 
  cat('Optioning: ',symbol,'\n')
  document=fromJSON(txt=paste0('https://query2.finance.yahoo.com/v7/finance/options/',symbol))
  dates = document$optionChain$result$expirationDates
  # options = tryCatch(do.call(rbind,lapply(dates[[1]], fetchOptionsChain,stk=symbol)), error = function(e) e)
  if(length(dates[[1]])>0){
            options = lapply(dates[[1]], function(dd) { tryCatch(fetchOptionsChain(dd,stk = symbol), error = function(e){data.table(inTheMoney=NA,OS=NA,OI=NA)} ) } )
            options = rbindlist(options, fill = T)
            options$symbol <-symbol
            options[, price := (bid+ask)/2]
            options[, spread := round((ask-bid)/price,2)]
            setnames(options,c('contractSymbol','openInterest','impliedVolatility','inTheMoney'), c('OS','OI','IV','ITM') )
          options[,-c('bid'  , 'ask' ,'contractSize','change','percentChange','expiration')]
          } else { data.table(symbol=symbol ,strike=NA, OI=NA,volume=NA,type=NA ,bid=NA,ask=NA,expiry = NA,IV= NA)}
}

# getOptions('QS')

calculateGreeks<-function(OS,stockPrice,dailyVolatility,rf =0.029,dy=0){
  if(is.na(dy)) dy=0
  daysToMaturity=as.numeric(as.Date(gsub("*\\D[[:digit:]]*$","",gsub(pattern="(^[[:alpha:]]+)","",OS)),format = "%y%m%d")-Sys.Date())
  yrs =daysToMaturity/365
  XP = str_extract(OS,'[[:digit:]]{8}$') %>% parse_number()/1000  
  op = GBSCharacteristics(TypeFlag =   if( str_sub(OS,-9,-9)=="C") "c"  else "p",
                          S = stockPrice, X = XP, Time = yrs, r = rf, b = rf-dy, sigma = dailyVolatility*sqrt(250))
  op$theta = op$theta/365
  return (op)
}
vcalculateGreeks = Vectorize(calculateGreeks)

IV<-function(OS,marketPrice,stock=NULL,stockPrice,rf=0.017, dy=0,vlty=NULL,...){ ## vlty = daily vlty in fraction..which period ?
  daysToMaturity = as.numeric(as.Date(gsub("*\\D[[:digit:]]*$","",gsub(pattern="(^[[:alpha:]]+)","",OS)),format = "%y%m%d")-Sys.Date())
   yrs =daysToMaturity/365
  XP = str_extract(OS,'[[:digit:]]{8}$') %>% parse_number()/1000  
  iv=tryCatch(GBSVolatility(price=marketPrice, TypeFlag =   if( str_sub(OS,-9,-9)=="C") "c"  else "p",
                            S=stockPrice, X=XP, Time=yrs, r=rf, b=rf-dy), error=function(x)NA)
  # greek = calculateGreeks(OS,stockPrice,vlty,rf,dy)  
  greek = calculateGreeks(OS,stockPrice,iv/sqrt(240),rf,dy) ## Feeding iv into the formula
  # greek = vcalculateGreeks(OS,stockPrice,iv/sqrt(240),rf,dy) ## Feeding iv into the formula  
  data.table(OS, IV=unlist(iv),delta=round(greek$delta,2),vega=round(greek$vega,2),
             optionPrice=round(greek$premium,2), price= marketPrice )
}
## IV-calls-greeks
# IV('ZTO211217C00020000',marketPrice=9.27,stockPrice=29,vlty = 0.01)
vIV = Vectorize(IV)

getParityPrice<-function(os,OFrame){ # Parity Prcing 
  if( substr(os,nchar(os)-8, nchar(os)-8)=="C"){ #Call Price 
    substr(os, nchar(os)-8, nchar(os)-8) <- 'P'  
    parityPrice= OFrame[OS==os]$stockPrice- OFrame[OS==os]$Strike/(1+rf)+ OFrame[OS==os,.(pairPrice=mean(c(Bid,Ask)))]$pairPrice
  }else {   # Put Price
    substr(os, nchar(os)-8, nchar(os)-8)<-'C'
    parityPrice= -OFrame[OS==os]$stockPrice + OFrame[OS==os]$Strike/(1+rf)+ OFrame[OS==os,.(pairPrice=mean(c(Bid,Ask)))]$pairPrice
  }
  return(ifelse(is.null(parityPrice),NA,round(parityPrice,2)))
}

fetchOptions<-function(stock,exp= NULL){  # quantmod option chain 
  # return(getOptionChain(Symbols=stock,Exp=exp))
  opList=tryCatch(quantmod::getOptionChain(Symbols=stock,Exp=exp), 
                  error=function(x){list(symbol=list(dat= data.table( Strike=NA,  Last=NA,   Chg=NA,   Bid=NA,   Ask=NA, Vol=NA,   OI=NA))); })
  # options<-do.call(rbind,lapply(getOptionChain(Symbols=stock,Exp=exp), function(x) do.call(rbind, x))) 
  # options<-do.call(rbind,lapply(opList, function(x) do.call(rbind, x))) 
  options = rbindlist(lapply(opList, function(x) do.call(rbind, x)))
  # options$OS<-gsub("^.*\\.","",rownames(options)) # option symbols
  options$OS =  do.call(c,lapply(opList, function(x) {c(row.names(x[[1]]),row.names(x[[2]])) }) )
  options$stock <- gsub(pattern="[[:digit:]]*\\D[[:digit:]]*$","",options$OS)
  
  return (options)  ## returns DT 
}
# fetchOptions('SNAP',exp = '2020')

getBatchOptions <-  function(tik,exp='2020'){  # quantmod option chain 
  # chain = lapply(tik ,function(tk) tryCatch(fetchOptions(tk,exp),error=function(x) data.table(stock=tk,Bid =NA,   Ask =NA)) ) %>% rbindlist(fill = T)
  chain = mclapply(unique(tik),
                   function(tk) tryCatch(fetchOptions(tk,exp),error=function(x) data.table(stock=tk,Bid =NA,   Ask =NA))
                   , mc.cores = parallel::detectCores()-1 ) %>% rbindlist(fill = T)
    chain[, price := (Bid+Ask)/2][]
  chain[, spread := (Ask-Bid)/price][]
  chain
}
# vgetBatchOptions = Vectorize(getBatchOptions,vectorize.args='tik')


# getBatchOptions(c('ba','intc'),exp='2020')
# getBatchOptions('SNAP',exp='2020')

getfrontoptions<-function(stock){  # quantmod option chain 
    # print(stock)
  oplist= tryCatch(quantmod::getOptionChain(Symbols=stock),  error=function(x){ list(data.table(rn=str_c(stock,'_'),symbol=stock,Bid=NA,Ask=NA,type=NA)) })
  if(length(oplist)==0){ return(data.table(rn=str_c(stock,'_'),symbol=stock,Bid=NA,Ask=NA,type=NA))}
  oplist= Map(as.data.table,oplist,keep.rownames=T) %>% rbindlist(fill = T)  ## sometime list can be empty
  if(sum(str_detect(names(oplist),pattern = 'Bid' ))>0 &  sum(str_detect(names(oplist),pattern = 'Ask' ))>0 ){
      oplist$symbol <- gsub(pattern="[[:digit:]]*\\D[[:digit:]]*$","",oplist$rn)
      oplist[, price := (Bid+Ask)/2]
      oplist[, spread := (Ask-Bid)/price]
      oplist$type = ifelse(gsub("[[:digit:]]*$","",gsub(pattern="(^[[:alpha:]]+)\\d{6}","",oplist$rn))=="C",'c','p') 
  return(oplist)
  } else {  data.table(rn=str_c(stock,'_'),symbol=stock,Bid=NA,Ask=NA,type=NA)}
}
vgetfrontoptions = Vectorize(getfrontoptions,vectorize.args='stock',SIMPLIFY = F)

# getfrontoptions('AAPL')
# # vgetfrontoptions('SNAP')
# vgetfrontoptions(c('ba','intc','AAMC') ) %>% rbindlist(fill=T)

## Options-tradier ----------------------------------------------------------------------------------------------
getPCRatio <- function(tik){ ## only front week 
   os = suppressWarnings(getOptionsSet_tradier(tik,filter = T)[,.(vol = sum(volume), oi=sum(OI)),type])
   (os[type=='P',-c('type')]/os[type=='C',-c('type')]) %>% 
     cbind(tik=tik)
}
# Volume P/C ratio below 0.75 signals high levels of bullish sentiment, so it’s considered bearish from a contrarian viewpoint. Between 0.75 and 1.00 is neutral.
# Above 1.00 indicates high levels of bearishness and is considered bullish by contrarians.
 # getPCRatio('SE')

getOpQuotes = function(symbol,...){  ## this is options-symbols not stk // send vector of at least 2
  ## payload restricted to 10K quotes , unless streaming enabled / chunk it ..
  symbol=  symbol %>% na.omit() ; print(paste('#Quoting:',length(symbol)))
  symbol.spl = split(symbol, ceiling(seq_along(symbol)/10000))  
  res = map(symbol.spl, function(spl){ 
            spl ->> tp1
            ret = content(POST(url='https://api.tradier.com/v1/markets/quotes', 
                         add_headers( Authorization='Bearer HUvSnb0uwwcBfszEGC0rWKRVqOQb', Accept='application/json',`Content-Type`='application/x-www-form-urlencoded' ),
                         body  = paste0('symbols=',spl %>% str_c(collapse  = ',') ,'&greeks=false') ## trim NA if so ..
                ))
            ret ->> tp
            if(class(tp)=='list') ret$quotes$quote %>%    rbindlist(fill = T)
            else { data.table(symbol=ret)}
      }) %>%  #   return(res)
    # select(symbol,last,bid, ask ,volume ,strike,change_percentage,open_interest,expiration_date,option_type ,root_symbol) %>% 
    rbindlist(fill = T) %>% 
    .[, price := (bid+ask)/2] %>%
    .[, spread := round((ask-bid)/price,2) ]  
  print('#Quoting:..Over')
  res[]
}
# getOpQuotes(out$OS)
# getOpQuotes(c('QCOM211112P00152500','CRAP211112P00108000'))

getOpQuotes2 = function(os,...){
  #   ## payload restricted to 5K quotes due to greeks, unless streaming enabled / chunk it ..
  os=  os %>% na.omit() ; print(paste('#Quoting:',length(os)))
  os.spl = split(os, ceiling(seq_along(os)/5000))
  res =   map_df(os.spl, function(spl){
    res_spl= content(POST(url='https://api.tradier.com/v1/markets/quotes',
                 add_headers( Authorization='Bearer HUvSnb0uwwcBfszEGC0rWKRVqOQb', Accept='application/json',`Content-Type`='application/x-www-form-urlencoded' ),
                 body=paste0('symbols=',spl %>% str_c(collapse  = ',') ,'&greeks=true'))  )
    
    map_df(res_spl$quotes$quote , function(x) 
      unlist(x) %>%  enframe %>% cbind(x$symbol) %>% as.data.table ) %>% 
      setnames(c('key','value','os')) %>% 
      dcast(os ~ key) %>% 
      type_convert %>% suppressMessages %>% 
      transmute(os,price=(bid+ask)/2,spread=(ask-bid)/price, per_change=change_percentage,expiry=expiration_date,delta=greeks.delta,strike,
                gamma=greeks.gamma ,iv = greeks.mid_iv,theta=greeks.theta, volume = last_volume, OI=open_interest,type=option_type,root=root_symbol) %>% 
      mutate(across(where(is.numeric),round,2))
  })   
 res
}
 # getOpQuotes2( c('RIOT220121P00003000' ,'GRWG220414C00005000'))
  # getOpQuotes2(out$OS)

# getOpQuotes2 = function(os,...){  ## 
#   ## payload restricted to 10K quotes , unless streaming enabled / chunk it ..
#   os=  os %>% na.omit() ; print(paste('#Quoting:',length(os)))
#   os.spl = split(os, ceiling(seq_along(os)/10000))  
#   
#   res =   map_df(os.spl, function(spl)
#             content(POST(url='https://api.tradier.com/v1/markets/quotes',
#                          add_headers( Authorization='Bearer HUvSnb0uwwcBfszEGC0rWKRVqOQb', Accept='application/json',`Content-Type`='application/x-www-form-urlencoded' ),
#                          body  = paste0('symbols=',spl %>% str_c(collapse  = ',') ,'&greeks=false') ## trim NA if so ..
#             ))$quotes$quote ) %>% as.data.table() %>%
#     .[, price := (bid+ask)/2] %>%
#     .[, spread := round((ask-bid)/price,2) ] %>% 
#     .[]
#    print('#Quoting:..Over')
# res
# }
# tp = c('QCOM211112P00152500','ABBV211112P00108000')
# getOpQuotes2('RIOT220121P00003000')




getOpSymbols2 = function(symbol,fdays=90,...){ # this is stk-symbol(not vector) , ... to absorb addl parameters
  url = paste0('https://api.tradier.com/v1/markets/options/lookup?underlying=',symbol)
  op_sym = 
    content(GET(url,add_headers( Authorization='Bearer HUvSnb0uwwcBfszEGC0rWKRVqOQb', Accept='application/json' )
    )) %>% unlist() %>% as.data.table()  
  if(nrow(op_sym)==0) return(data.table(symbol=symbol))
  names(op_sym) <- 'OS'
  op_sym$OS = as.character(op_sym$OS)
  op_sym = 
    op_sym  %>% 
    # setnames('.','OS') %>% 
    .[,symbol := symbol]    %>% 
    # .[,stock_price := ClosePrice]    %>% 
    .[, stock_V1 := str_remove(OS,'[[:alnum:]]{15}$')] %>% 
    .[,strike := str_extract(OS,'[[:digit:]]{8}$') %>% parse_number()/1000] %>% 
    .[,type := str_sub(OS,-9,-9)] %>% 
    .[, expiry := str_extract(str_remove(OS,symbol),'^[[:digit:]]{6}') %>% readr::parse_date(format='%y%m%d')] %>% 
    # .[ strike %between% list(.7*stock_price,1.3*stock_price) ] %>%   ## +/- 30%
    .[ expiry  %between% list(Sys.Date() ,Sys.Date()+fdays )]
  op_sym
}
# getOpSymbols2('RIOT')

getOpSymbols = function(symbol,ClosePrice,fdays=90,...){ # this is stk-symbol(not vector) , ... to absorb addl parameters
  print(symbol)
  url = paste0('https://sandbox.tradier.com/v1/markets/options/lookup?underlying=',symbol)
  op_sym = 
    content(GET(url,add_headers( Authorization='Bearer Q9y3eVIWiM1g1ehlQLE4yfcpGwEO', Accept='application/json' )
    ))$symbols[[1]]$options %>% unlist %>% as.data.table()  
  if(nrow(op_sym)==0) return(data.table(symbol=symbol))
  op_sym = 
    op_sym  %>% 
    setnames('.','OS') %>% 
    .[,symbol := symbol]    %>% 
    .[,stock_price := ClosePrice]    %>% 
    .[, stock_V1 := str_remove(OS,'[[:alnum:]]{15}$')] %>% 
    .[,strike := str_extract(OS,'[[:digit:]]{8}$') %>% parse_number()/1000] %>% 
    .[,type := str_sub(OS,-9,-9)] %>% 
    .[, expiry := str_extract(str_remove(OS,symbol),'^[[:digit:]]{6}') %>% readr::parse_date(format='%y%m%d')] %>% 
    .[ strike %between% list(.7*stock_price,1.3*stock_price) ] %>%   ## +/- 30%
    .[ expiry  %between% list(Sys.Date() ,Sys.Date()+fdays )]
  op_sym
}
# temp = pmap_dfr(stocks[1:10,.(symbol,ClosePrice)] , getOpSymbols) 
## > 10 puts with front week 
# temp[expiry<(Sys.Date()+7) &  type=='P'][,.N,symbol][N>10][order(-N)] 
# pmap_dfr( xls[Remarks=='SP'][, symbol := str_remove(symbol,'[[:alnum:]]{15}$')] , getOpSymbols) %$%  getOpQuotes(V1) %>% .[symbol=='RH201218P00470000']

filterFront = function(OSframe, ty=c('P','C'),filter=T){
  if(filter){
  print('filter applied: > 10 puts/calls in front week over +/- 30% Strike') 
  ls = OSframe[expiry<(Sys.Date()+7)  &  type %in% ty][,.N,symbol][N>10]$symbol
  OSframe[symbol %in% ls & expiry<(Sys.Date()+7) &  type %in% ty]
  } else {OSframe}
}

getOptionsSet_tradier <- function(stks,fwd=60,filter=T, del=NULL){ #  vec of stocks stks= c('RH','AA','C','CRAP','TWTR')
  sdata = getStockData(unique(stks) %>% sort) %>% as.data.table()
  out =  sdata[,.(symbol,ClosePrice)] %>% 
          pmap_dfr( getOpSymbols2,fdays = fwd) %>%  
           filterFront(filter = filter)  
  out ->> out
  out =  getOpQuotes2(out$OS) %>% 
         merge(sdata,by.y='symbol',by.x='root')  %>% 
    #  .[,.( symbol,  price, spread,  strike ,OI=open_interest, expiration=expiration_date,type=  option_type , stock=root_symbol)] %>% 

    # cbind(
    #   pmap_dfr(., function(symbol,price,ClosePrice,vlty,DividendYield,...)
    #               IV(OS=symbol,marketPrice=price,stockPrice=ClosePrice,vlty=vlty,dy=0,rf=0))
    # ) %>%
    .[,daystoexpiry := as.numeric(expiry - Sys.Date()+1)] %>% ## +1 avoid div 0
    # .[, root := str_remove(symbol,'[[:alnum:]]{15}$')] %>%
    # .[,strike := str_extract(symbol,'[[:digit:]]{8}$') %>% parse_number()/1000] %>%
    .[,type := str_sub(os,-9,-9)] %>%
    .[, time_val := if_else(type=='P',if_else(strike<ClosePrice,price,price+ClosePrice-strike), ## for P
                            if_else(strike>ClosePrice,price,price-ClosePrice+strike)) %>% round(2)] %>% ## for C
    .[, tval_z := round(100*time_val/ClosePrice/daystoexpiry,2)] %>% 
    .[,.(os,root,ClosePrice,type,strike,price,spread,OI,volume,IV=round(iv,2),IVVR=round(iv/(16*vlty),2),delta, tval_z,daystoexpiry)] #  16 for annualized
    
    if(is.null(del))  out[abs(delta) %between% c(0.01,0.99)] ## all sane values 
    else out[abs(delta) %between% c(0.01,0.99)][delta %between% c(del-0.1,del+0.1)][order(daystoexpiry)]
}
# getOptionsSet_tradier('FB',del= -0.1)  ## del is +ve for calls 
 # getOptionsSet_tradier(soi_sp$symbol)



## Older --------------------------------------------------------------------------------

corPairs <- function(X, dfr = nrow(X) - 2) {
  R <- cor(X)
  above <- row(R) < col(R)
  r2 <- R[above]^2
  Fstat <- r2 * dfr / (1 - r2)
  R[above] <- 1 - pf(Fstat, 1, dfr)
  cor.mat <- t(R)
  cor.mat[upper.tri(cor.mat)] <- NA
  cor.mat
}  # Cal pairs of correlations between variables of DF

eventgenerate<-function(stkhist,event,pricedrop){
  stkhist = stkhist[order(as.Date(stkhist$date)),] #order by dates
  if(pricedrop == TRUE){
    idx = which(stkhist[which(stkhist$close<event)-1,]$close>=event) # records of a day earlier > event 
    newdf = (stkhist[which(stkhist$close<event)-1,])
  }else{
    # Price Surge
    idx = which(stkhist[which(stkhist$close<event)-1,]$close<event) # records of a day earlier < event 
    newdf = (stkhist[which(stkhist$close<event)-1,])
  }
  return(newdf[idx,])
}

backTest<-function(stkvalues,orders){
  # Process: Signals -> holdings -> action (buy,sell)
  
  symlist = unique(orders$asset) # This is for multiple tickers
  
  holdings = matrix(nrow= if(is.null(nrow(stkvalues))){ length(stkvalues)},ncol=length(symlist))
  colnames(holdings)<-c(as.character(symlist))
  
  txmatrix<-apply(orders,2,cumsum)  
  cbind(txmatrix,ifelse(txmatrix[,"signal"]<0,0,txmatrix[,"signal"]))
  cbind(txmatrix,ifelse(txmatrix[,"signal"]>1,1,txmatrix[,"signal"]))
  
  
  df<- orders ; colnames(df)<-c("asset","holdings") ;    df[,"holdings"]<-0 
  
  for( sig in 2:length(orders$signal)){
    
    df[,"holdings"][sig]=df[,"holdings"][sig-1]+orders$signal[sig]
    if(df[,"holdings"][sig]<0)  df[,"holdings"][sig]<-0
    if(df[,"holdings"][sig]>1)  df[,"holdings"][sig]<-1
  }
  
  # excellent results but data snooping ?? -> seems to have rectified in NB.R file 
  action<- rbind(data.frame(action=diff(df$holdings)),data.frame(action=0))
  df= data.frame(action,df,stkvalues,holdingValue=stkvalues*df[,"holdings"]) # diff is aligned left
  
  df <-data.frame(df,realizedValue=cumsum(-1*df$holdings*orders$signal*stkvalues),signal=orders$signal)
  # ggplot(aes(x=as.Date(row.names(df))),data=df) + geom_line(aes(color="holdingValue",y=holdingValue)) + geom_line(aes(color="realizedValue",y=realizedValue))
  return (df)
}  # function 

getPortfolioStats<-function(dailyreturns, pfweights=1:NCOL(dailyreturns)){
  
  pf.ret  = pfweights%*%t(dailyreturns) # daily returns
  pf.sd = sd(pf.ret,na.rm=TRUE)  # Volatility - seems ok 
  pf.mean = mean(pf.ret,na.rm=TRUE)  # Avg daily Return 
  pf.ret.cumm = rowSums(pf.ret,na.rm=TRUE)/(ncol(pf.ret)) # Average Daily Return - Aggregate way
  pf.sharpe =sqrt(252)*(pf.mean/pf.sd)  # Annualized
  df=data.frame(pf.sd, pf.mean,pf.sharpe)
  
  return(df)
  
  #  return(as.data.frame(c(pfsd=pf.sd, pfmean=pf.mean, pfsharpe=pf.sharpe)))
}

calculateFibRetrace<-function(prices){
  ## Calculate Fibonacci Retracements over last 90 periods 
  hi <- last(Hi(prices),90) 
  lo <- last(Lo(prices),90) 
  FR100 <- max(hi) 
  FR0 <- min(lo) 
  last90 <- last(prices,90) 
  last90$FR100 <- FR100 
  last90$FR0 <- FR0 
  last90$FR79 <- FR100 - (FR100 - FR0) * 0.786; 
  last90$FR62 <- FR100 - (FR100 - FR0) * 0.618; 
  last90$FR50 <- FR100 - (FR100 - FR0) * 0.500; 
  last90$FR38 <- FR100 - (FR100 - FR0) * 0.382; 
  last90$FR24 <- FR100 - (FR100 - FR0) * 0.236; 
  # last90$FR124 <- FR100 + (FR100 - FR0) * 0.236; 
  
  chart_Series(last90, theme=chart_theme(),name=" (OHLC)") 
  add_Series(last90[,6],on=1) 
  add_Series(last90[,7],on=1) 
  add_Series(last90[,8],on=1) 
  add_Series(last90[,10],on=1) 
  add_Series(last90[,11],on=1) 
  add_Series(last90[,12],on=1) 
  add_Series(last90[,9],on=1) 
  
}

getBollingerValue<-function(asset,startDate,endDate,lookback=20){
  # Notes: %B = (Price - Lower Band)/(Upper Band - Lower Band)
  prices =  HLC(getSymbols(asset,from = startDate, to = endDate,env=NULL))
  bb= BBands(prices,n=lookback)
  return(na.omit(merge(bb,prices)))
}

getMultipleBollingerValues<-function(asset,endDate,lookback,idxrange){
  
  #   endDate is the daet at which BValue is done
  #   startDate is begin of stock history
  #   Min 2 assests are required   
  endDate = as.Date(endDate,format="%m/%d/%Y")
  startDate = endDate -2*idxrange- 2*(lookback)
  
  st = mapply(SIMPLIFY=FALSE ,as.character(asset)  ,FUN=function(x)tryCatch(getStockHist(stk=x,sdate=startDate,edate=endDate,pricetype="Adj.Close") ,error=function(e)return(NA)))
  
  stkval= do.call("cbind",st)
  asset.vec.rv = names(st)
  stkval= stkval[order(stkval[1],decreasing=T),]
  
  rollsd = sapply(c(1:idxrange),function(x){apply(stkval[x:(x+lookback-1),paste0(asset.vec.rv,".price")], 2, sd)})
  rollmean = as.matrix(sapply(c(1:idxrange),function(x){apply(stkval[x:(x+lookback-1),paste0(asset.vec.rv,".price")], 2, mean)}))
  
  stkprices = as.matrix(sapply(c(1:idxrange),function(x){stkval[x,paste0(asset.vec.rv,".price")]}))
  #   df = t(as.data.frame((as.numeric(stkprices)-rollmean)/rollsd) )
  #   cbind(df,stkval[1])
  return((as.numeric(stkprices)-rollmean)/rollsd) 
}

getCrossNew<-function(asset="SPY",endDate = Sys.Date(),fwdDays=6){
  rollingAvgShort = 40
  rollingAvgLong = 200
  # idxrange = 45 # window of observations from end date: 60 = Quarter 
  proximity = 0.1 # tolerance: distance threshold betwee long and short averages
  slopeSigF = 0.001 # Slope cannot be less than this number to be considered
  
  startDate = endDate -365 -rollingAvgLong -30 # we need to take more dates as tradings days are less than calendar days
  prices =  Cl(getSymbols(asset,from = startDate, to = endDate,env=NULL))
  pricesTesting = prices[(length(prices)-30):length(prices)]
  pricesTraining = prices[1:(length(prices)-30)]
  
  rollmeanShort = EMA(pricesTraining,rollingAvgShort)
  rollmeanLong = EMA(pricesTraining,rollingAvgLong)
  potCases = (abs((rollmeanShort-rollmeanLong)/rollmeanLong) < proximity) *(rollmeanShort-rollmeanLong)
  dt=data.table(dates=index(potCases),diff=potCases,slopeShort= ROC(rollmeanShort),slopeLong=ROC(rollmeanLong),symbol=asset,pR=prices/shift(prices, fwdDays, type='lead'))
  setkey(dt,dates)
  colnames(dt)<-c("dates","diff.EMA","slopeShort","slopeLong","symbol",paste0("priceRatio.",fwdDays))
  
  dt = dt[(complete.cases(dt) &  diff.EMA != 0)]
  pc = paste0("priceRatio.",fwdDays)
  dt[,priceChange:=ifelse((pc)>1L,"UP","DN")]
  
  dt[,priceChange:=ifelse(dt[[6]]>1.0,"UP","DN")]  # Col 6 is hard coded 
  dt$cross =  ifelse(dt$diff.EMA>0 & dt$slopeShort<0 &  dt$slopeLong<0,"D",ifelse(dt$diff.EMA<0 & dt$slopeShort>0 &  dt$slopeLong>0,"G",""))
  
  # dt[,priceChange:=ifelse(eval(as.expression(paste0("priceRatio.",fwdDays)))>1.0,"UP","DN")]
  #  .(p5=p1/p2,p1=prices[(dt[,dates])],p2=prices[(dt[,dates]+5)])
  return(dt)
}
