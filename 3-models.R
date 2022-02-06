## tidymodels for options
# library(xgboost)
library(tidymodels)
library(embed)
library(vip)
library(usemodels)

applyModels <- function(df,rec=NULL,mods,...){
  # df,recipe and list of models to apply 5-folds,grid=7 (other can be done), out: list of last fit
  target <- rec %>%   pluck("term_info") %>%  dplyr::filter(role == "outcome") %>%    pull(variable) %>%  sym()
  splits <- initial_split(df, prop = 0.80)
  # folds =   bootstraps(training(splits), times = 200) ## only one is needed..
  print('create Wfs')
  wfs <- map(mods , function(x) workflow() %>%  add_recipe(rec) %>%   add_model(x )) 
  print('tuning..') ;    future::plan(future::multisession, workers = 3) ## or create a custom grid-tune
  tuned = furrr::future_map(wfs, ~tune_grid(.x , resamples = training(splits) %>%  vfold_cv(v = 5),  grid = 7,
                                            metrics = metric_set( roc_auc, specificity))
  ) %>% 
    map(~select_best(.x,metric = 'roc_auc'))
  
  final_fits = map2(wfs,tuned, ~finalize_workflow(.x,.y  ))   %>% map(~last_fit(.x,splits) )
  return(final_fits)
}  

svm_spec = svm_rbf( mode = "classification",engine = "kernlab", cost = tune(),rbf_sigma = tune(),   margin = tune() )
xgboost_spec <-   boost_tree(trees = 1000, min_n = tune(), tree_depth = tune(), learn_rate = tune(), loss_reduction = tune(), sample_size = tune()) %>% 
  set_mode("classification") %>%  set_engine("xgboost") 
glm_spec <- logistic_reg(  mode    = "classification",  penalty = tune(),  mixture = tune() ) %>%   set_engine("glmnet")
mlp_spec = mlp( mode = "classification",epochs = 100, hidden_units = tune(), dropout = tune()) %>%  set_engine("keras",verbose=0)
rf_spec <-  rand_forest(   mode = "classification", mtry = tune(), trees = 1500,  min_n = tune() ) %>% set_engine("ranger", importance = 'permutation')
tree_spec = decision_tree( cost_complexity = tune(), tree_depth = tune() ) %>%  set_engine("rpart") %>%   set_mode("classification")
# cubist_spec <- cubist_rules(committees = tune(), neighbors = tune()) %>%     set_engine("Cubist") 

# tickers = read.xlsx('https://onedrive.live.com/download?cid=13C9A0CA3A18CA60&resid=13C9A0CA3A18CA60%2140997&authkey=AC_TPn19sar8u4M',detectDates = T) %>% as.data.table() %>% 
#  .[ !is.na(IV_flag), .(symbol=Symbol,actionDate,ClosePrice,ShortFloat,ShortRatio)]
 


## Load data for model IN: .keep_symbols ===============================================================

data = as.data.table(computeCandles(  tickers[Symbol %in% keep_symbols ]$Symbol ) )[index>Sys.Date()-45] %>% ## only valid symbols
  select(-Open,-High,-Low,-Close,-Volume,-ma3,-fwd1,-fwd2)
  # select(index,symbol,returns,vrank,ma3_sl,size,prev_size,pos,color,openHigher,vlty5,volz,relativeSize,closedHigh,prev_ret,reversal)  

data = left_join(data,tickers[,.(symbol=Symbol,mcapz=scale(MCap),sector=Sector)],by='symbol') %>% 
        mutate(mcapz=if_else(is.na(mcapz.V1),0,mcapz.V1), sector = if_else(is.na(sector),'unk',sector)) %>% 
        select(-mcapz.V1)

data$reversal %>% table
data[index>Sys.Date()-3,-c('reversal')] %>% drop_na()
# ds =  na.omit(data) %>% slice_sample(n=5000)
# ds$reversal %>% table

# ds_up = rbind(data[move=='up']%>% slice_sample(n=1000),data[move=='na'] %>% slice_sample(n=2000))
# ds_dn = rbind(data[move=='dn']%>% slice_sample(n=1000),data[move=='na'] %>% slice_sample(n=2000))
# ds = ds_dn

# ds = drop_na(data); data[index>Sys.Date()-2]
## model - area..
rsp <- 
  recipe(formula = reversal ~ ., data =  na.omit(data)) %>% 
  update_role(index,symbol ,new_role = "id") %>%
  # step_string2factor(one_of(symbol, color, reversal)) %>% 
   step_novel(all_nominal(), -all_outcomes()) %>% 
  # step_other..
  step_zv(all_predictors()) %>% 
  step_dummy( all_nominal_predictors(), one_hot = TRUE) 

rsp
bake(prep(rsp),NULL) %>% glimpse()
## ranger 2x
final_fits = list(applyModels(na.omit(data) %>% slice_sample(n=5000),rsp,list(rf_spec))[[1]] , 
                  applyModels(na.omit(data) %>% slice_sample(n=5000),rsp,list(rf_spec))[[1]]  )

# final_fits = applyModels(ds,rsp,list(svm_spec,glm_spec,xgboost_spec,rf_spec))


final_fits %>%  map( ~collect_metrics(.x) ) # The metrics in fit are computed using the testing data.
# # extract_workflow(final_fits)
# final_fits %>% map( ~augment(.x)) << good
# # autoplot(tuned) %>% print() ;    select_best(tuned) %>% print()

# VIP 
map(final_fits, ~extract_workflow(.x) %>%   extract_fit_parsnip() %>%
      vip(num_features = 15, geom = "point"))

# Density
map(final_fits, ~augment(.x ) %>% 
      ggplot()+geom_density(aes(x= .pred_yes,color=reversal)) )

## Preds / mean, spread
preds = map(final_fits, ~extract_workflow(.x) ) %>% 
  map(function(x){ augment(x, data[index==Sys.Date(),-c('reversal')] %>% drop_na ) %>%
            bind_cols(model=extract_spec_parsnip(x)$engine) }) %>%   rbindlist() %>% 
             .[,.(pred_yes=mean(.pred_yes),pred_no= mean(.pred_no),
             spread_n= abs( .pred_no[2]-.pred_no[1]),
             spread_y= abs( .pred_yes[2]-.pred_yes[1]),
             .N),.(symbol,index)] %>% 
        left_join(stocks[,.(symbol,ma3_sl,actionDate)]) %>% 
        mutate(actionDate = if_else(actionDate<Sys.Date(),as.Date(NA),actionDate)) %>% 
        left_join(frontOptions[type=='C' & abs(delta) %between% c(0.5,0.8),.(root,os,ClosePrice,strike,price,spread,IVVR)],by=c('symbol'='root')) %>% 
        mutate(across(where(is.numeric),round,2))  
 
## REVERSAL : All in one PRED get_pred(); Reversal
preds %>%  # select(-pred_no,-spread_n) %>% 
  .[order(-pred_yes)] %>% .[pred_yes>0.7 |  pred_no < 0.3] %>%  # or %>% .[pred_no < 0.3]
  print(topn=150)

## Continuation::
preds %>% # select(-pred_yes,-spread_y) %>% 
  .[order(-pred_no)] %>% .[pred_no > 0.7 |  pred_yes < 0.3]

## by Symbol 

get_predictions<- function(ds){
  rsp <- 
    recipe(formula = move ~ ., data = ds) %>% 
    update_role(index,symbol ,new_role = "id") %>%
    step_dummy( all_nominal_predictors(), one_hot = TRUE) %>% 
    step_zv(all_predictors()) 
  
 applyModels(ds,rsp,list(rf_spec,glm_spec)) %>% 
  map( ~extract_workflow(.x) ) %>% 
    map(~augment(.x, data[index>Sys.Date()-2]) ) %>% 
    rbindlist()  %>% select(-.pred_class, -.pred_na,-move)
}  

preds = full_join(get_predictions(ds_up),get_predictions(ds_dn))
preds[.pred_up>0.65  | .pred_dn>0.65][order(symbol)]
preds[symbol=='BBY']  


## --- OLD REF--
xgboost_spec <- 
  boost_tree(trees = tune(), min_n = tune(), tree_depth = tune(), learn_rate = tune(), 
             loss_reduction = tune(), sample_size = tune()) %>% 
  set_mode("classification") %>% 
  set_engine("xgboost") 

xgboost_workflow <- 
  workflow() %>% 
  add_recipe(rsp) %>% 
  add_model(xgboost_spec) 

set.seed(80308)
xgboost_tune <-
  tune_grid(xgboost_workflow, resamples = boot_folds <- bootstraps(ds, times = 10,strata = reversal), 
            grid = grid_max_entropy(parameters(xgboost_spec), size = 7),
            metrics = metric_set(accuracy, roc_auc, sensitivity, specificity,mn_log_loss))


autoplot(xgboost_tune) + theme_light()
xgboost_tune %>% show_best('mn_log_loss')
final_fit = finalize_workflow(xgboost_workflow, xgboost_tune %>% select_best('mn_log_loss') ) %>% 
                 last_fit(split= initial_split(ds))
final_fit %>%    collect_metrics()
extract_workflow(final_fit)

xgboost_tune %>%
  collect_metrics() %>%
  mutate(tree_depth = factor(tree_depth)) %>%
  ggplot(aes(learn_rate, mean, color = tree_depth)) +
  geom_line(size = 1.5, alpha = 0.6) +
  geom_point(size = 2) +
  facet_wrap(~ .metric, scales = "free", nrow = 2) +
  scale_x_log10(labels = scales::label_number()) +
  scale_color_viridis_d(option = "plasma", begin = .9, end = 0)

# simpler_tree <- select_by_one_std_err(tree_rs,-cost_complexity, metric = "roc_auc")


## can save this for prediction later with readr::write_rds()
collect_predictions(final_fit) %>%
  conf_mat(reversal, .pred_class) %>%
  autoplot()

collect_predictions(final_fit) %>%
  roc_curve(truth = reversal,.pred_yes) %>% 
  ggplot(aes(x = 1 - specificity, y = sensitivity)) +
  geom_path() +
  geom_abline(lty = 3) +
  coord_equal() +
  theme_bw()

augment(final_fit ) %>% 
  ggplot()+geom_density(aes(x= .pred_yes,color=reversal))
        
augment(final_fit )[.pred_yes> .6] %>% conf_mat(reversal,.pred_class)
augment(final_fit )[.pred_yes> .6][order(-.pred_yes)]
predict(extract_workflow(final_fit), new_data = , type = "prob")
# I chose not to tune the random forest because they typically do pretty well if you give them enough trees.

extract_workflow(final_fit) %>%
  extract_fit_parsnip() %>%
  vip(num_features = 15, geom = "point")

extract_workflow(final_fit) %>% 
  extract_fit_parsnip() %>%
  vip( aesthetics = list(fill = tidyquant::palette_light()["blue"])) +   
  labs(title = " Model Importance -  Predictors") +   tidyquant::theme_tq() 


## ---
boosted_res <- last_fit(boosted_wf, tidy_split) ## eval on test set 
boosted_aug <- augment(final_boosted_model, new_data = amphibio[,-1])
final_boosted_model <- fit(boosted_wf, amphibio)

final_boosted_model %>%
  predict(bake(tidy_rec, new_data = tidy_test), type = "prob") %>%
  bind_cols(tidy_test) %>%
  roc_auc(factor(Order), .pred_Anura)


final_boosted_model %>%
  predict(bake(tidy_rec, new_data = tidy_test), type = "prob") %>%
  bind_cols(tidy_test) %>%
  roc_curve(factor(Order), .pred_Anura) %>%
  autoplot() 


 
 
 # collect_predictions(final_fit) %>% as.data.table() %>% .[.pred > .005] %>% .[order(- .pred)]
 collect_predictions(final_fit) %>% as.data.table() %$% table(.pred_class,returns)


# The predictions in fit are also for the testing data.
collect_predictions(final_fit) %>%
  ggplot(aes(returns, .pred_class)) +
  geom_abline(lty = 2, color = "gray50") +
  geom_point(alpha = 0.5, color = "midnightblue") +
  coord_fixed()
 
 
