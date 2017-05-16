

options(scipen = 999)
Sys.setenv(TZ="US/Eastern")
options("stringsAsFactors" = FALSE)

# library(tm)
# library(wordcloud)

library(parallel)
#library(AWS.tools)
#library(Snowball)
# library(RWeka)
# library(rJava)
# library(RWekajars)
# library(igraph)
# library(Rstem)
library(qdap)
library(plotrix)
# library(Rbbg)
# library(gtools)
library(quantmod)

# library(shiny)
library(RCurl)
# library(openNLP)
# library(stringi)
library(fastmatch)
library(date)
# library(Rsymphony)
library(caret)
library(doParallel)
# library(pROC)
# library(h2o)
# library(dygraphs)
# library(lsa)

# library(h2oEnsemble)
# library(SuperLearner)
# library(cvAUC)

library(datasets)
library(stats)

library(reshape)

# library(forecast)
# library(randomForest)
# library(multiPIM)
# library(party)
# library(readr)
# library(purge)




load_ticker_spot_datawj<-function(etf = 'XLK'){

	file<-sprintf("/data/%s_spot_features_data.rds", etf)
	dat<-readRDS(file)
	return(dat)

}



run_create_models<-function(){


	
	# tsks<-create_learning_models_queue()

	# tsks<-split(tsks, 1:nrow(tsks))
	# tests<-lapply(tsks, create_models_worker, cl = cl, namet = 'first', multi_p = TRUE)




}
create_models_worker<-function(tsk, load_current = FALSE, cl = NULL, namet = 'eleventh', multi_p = TRUE){


	# if(multi_p){
	# 	if(is.null(cl)){
	# 		print('spawning new workers')
	# 		flush.console()
	# 		num_workers = detectCores() -1
	# 		cl<-makeCluster(num_workers)
	# 		clusterEvalQ(cl,source("code/DaVinci/R/src/tim/TimTest.R"))
			
	# 	}else{
	# 		num_workers<-length(cl)
	# 	}
	# }else{
	# 	num_workers = 1
	# }


	if(.Platform$OS.type == "unix") {
		lnx<-TRUE
	} else {
		lnx<-FALSE
	}
	etfs<-"XLK"
	dat<-load_ticker_spot_data(etfs)
	
	years.avail<-unique(dat$test_year)
	test_years<-tail(years.avail, -4)

	res_yrs<-list()

	ret_var = "stk_ret_10"
	input_vars = c('psl_tre_ewma_5_sig1','spreads_psl_tre_ewma_5_ns_sig1','psl_tre_ewma_30_sig1','spreads_psl_tre_ewma_30_ns_sig1')
	
	nday_ret = unlist(strsplit(ret_var, "_"))
	nday_ret = as.numeric(nday_ret[length(nday_ret)])

	 #get_etfs()

	
	
	for(q in 1:length(test_years)){
	
		tst_yr<-test_years[q]
		trn_yrs<-years.avail[years.avail != tst_yr]
		trn_yrs<-trn_yrs[trn_yrs<tst_yr]
		trn_yrs<-tail(trn_yrs, 5)
		dat_sub<-dat[dat$test_year %in% c(trn_yrs, tst_yr),]
		tkrs<-unique(dat_sub$ticker)
		tkrs<-tkrs[order(tkrs)]

		wgts<-rep(1, length(tkrs))
		wgts<-data.frame(t(wgts))
		names(wgts)<-tkrs

		dat_tr <- dat[dat$test_year %in% trn_yrs,]
		dat_tst <- dat[dat$test_year %in% tst_yr,]

		mxdattr<-as.data.frame(dat_tr[,c(ret_var,'tr_date','ticker', input_vars)])
		mxdattst<-as.data.frame(dat_tst[,c(ret_var, 'tr_date','ticker', input_vars)])
	
		cat_vars<-input_vars[grepl('cat_', input_vars)]
		num_vars<-input_vars[!grepl('cat_', input_vars)]
			
		na_quantile = FALSE
		center_data = FALSE
		norm_data = 'REG'

		if(na_quantile | center_data | norm_data == 'NORM'){
			for(j in 1:length(num_vars)){

				if(na_quantile){
					mxdattr[,num_vars[j]]<-NAQuantiles(mxdattr[,num_vars[j]], .01, .99) 
					if(tst_yr !=0) mxdattst[,num_vars[j]]<-NAQuantiles(mxdattst[,num_vars[j]], .01, .99) 
				
				}
				
				if(center_data){
					mxdattr[,num_vars[j]]<-mxdattr[,num_vars[j]] - mean(mxdattr[,num_vars[j]], na.rm = TRUE)
					if(tst_yr !=0) mxdattst[,num_vars[j]]<-mxdattst[,num_vars[j]] - mean(mxdattst[,num_vars[j]], na.rm = TRUE)
				}
				if(norm_data == 'NORM'){
					mxdattr[,num_vars[j]]<-norm_series(as.numeric(mxdattr[,num_vars[j]]), sig.lim = 3, center.zero = TRUE)  #mxdattr[,num_vars[j]] - mean(mxdattr[,num_vars[j]], na.rm = TRUE)
					if(tst_yr !=0) mxdattst[,num_vars[j]]<-norm_series(as.numeric(mxdattst[,num_vars[j]]), sig.lim = 3, center.zero = TRUE)  #mxdattst[,num_vars[j]] - mean(mxdattst[,num_vars[j]], na.rm = TRUE)
				}
				
				if(model_package == 'h2o'){
					dat_h2o_tr[,num_vars[j]]<-as.h2o(mxdattr[,num_vars[j]])
					if(tst_yr !=0) dat_h2o_tst[,num_vars[j]]<-as.h2o(mxdattst[,num_vars[j]])
				}else{
					dat_tr[,num_vars[j]]<-as.numeric(mxdattr[,num_vars[j]])
					if(tst_yr !=0) dat_tst[,num_vars[j]]<-as.numeric(mxdattst[,num_vars[j]])
				}		
				
			}

		}
		if(na_quantile){
			mxdattr[,ret_var]<-NAQuantiles(mxdattr[,ret_var], .01, .99) 
		}
				
		##normalize returns for sd of ticker returns
		sd_ticker<-aggregate(mxdattr[,ret_var], by = list(mxdattr$ticker), sd, na.rm = TRUE)
		names(sd_ticker)<-c('ticker','sd_ret')
		mxdattr<-merge(mxdattr, sd_ticker, by = 'ticker', all.x = TRUE)
		mxdattr[,ret_var]<-mxdattr[,ret_var]/mxdattr$sd_ret
		
		if(center_data){
			mxdattr[,ret_var]<-mxdattr[,ret_var] - mean(mxdattr[,ret_var], na.rm = TRUE)
		}

		if(norm_data=='NORM'){
			mxdattr[,ret_var]<-norm_series(as.numeric(mxdattr[,ret_var]), sig.lim = 3, center.zero = TRUE)  #mxdattr[,num_vars[j]] - mean(mxdattr[,num_vars[j]], na.rm = TRUE)
		}
			

		dat_tr[,ret_var]<-mxdattr[,ret_var]
		
		mxdattr$buy_sell<-"BUY" #as.character(mxdattr$buy_sell)
		mxdattst$buy_sell<-"BUY" #as.character(mxdattst$buy_sell)

		mxdattr$buy_sell[mxdattr[,ret_var]>=0]<-"BUY"
		mxdattr$buy_sell[mxdattr[,ret_var]<0]<-"SELL"
	 	mxdattst$buy_sell[mxdattst[,ret_var]>=0]<-"BUY"
		mxdattst$buy_sell[mxdattst[,ret_var]<0]<-"SELL"

		dat_tr[,'buy_sell']<-as.factor(mxdattr$buy_sell)
		dat_tst[,'buy_sell']<-as.factor(mxdattst$buy_sell)
		
		if(length(cat_vars)>0){
			for(i in 1:length(cat_vars)){
				if(tst_yr !=0) dat_tst[,cat_vars[i]]<-as.factor(dat_tst[,cat_vars[i]])
				if(!load_current){
					dat_tr[,cat_vars[i]]<-as.factor(dat_tr[,cat_vars[i]])
				}
			}
		}
			

		predict_type = 'class'
		if(predict_type == 'class'){
			opt_var<-'buy_sell'
		}else if(predict_type == 'regr'){
			opt_var<-ret_var
		}

		force_balance = FALSE
		if(force_balance){
			xx<-table(mxdattr$buy_sell)
			by<-xx[['BUY']]
			sl<-xx[['SELL']]
			mxdattr$keep<-0
			if(sl>by){
				mxdattr$keep[mxdattr$buy_sell == 'BUY']<-1
				yy<-which(mxdattr$buy_sell == 'SELL')
				mxdattr$keep[sample(yy, by)]<-1
			}
			if(by>sl){
				mxdattr$keep[mxdattr$buy_sell == 'SELL']<-1
				yy<-which(mxdattr$buy_sell == 'BUY')
				mxdattr$keep[sample(yy, sl)]<-1
			}

			if(predict_type =='class'){
				dat_tr[,opt_var]<-as.factor(dat_tr[,opt_var])
				if(tst_yr !=0) dat_tst[,opt_var]<-as.factor(dat_tst[,opt_var])
			
			}
			
			dat_tr[,'keep']<-mxdattr$keep
			dat_tr<-dat_tr[dat_tr$keep == 1,]
			mxdattr<-mxdattr[mxdattr$keep == 1,]
	
		}

			
		randomize_order = TRUE
		if(randomize_order){

			mxdattr$rando<-sample(1:nrow(mxdattr))
			mxdattst$rando<-sample(1:nrow(mxdattst))
			dat_tr[,'rando']<-as.numeric(mxdattr$rando)
			dat_tst[,'rando']<-as.numeric(mxdattst$rando)

			dat_tr<-dat_tr[order(dat_tr$rando), ]
			dat_tst<-dat_tst[order(dat_tst$rando), ]

			mxdattr<-mxdattr[order(mxdattr$rando),]
			mxdattst<-mxdattst[order(mxdattst$rando),]

		}
		
			# clst_num = i

		
		

		# loc.fldr<-<-sprintf("Shared_Data/ts_models/%s/%s/%s/%s/%s/%s/model/", model_package, nmt,tst_yr,ticker, type, model_id) #,max_depth,clst_num)
		
		model_package= 'caret'
		if(model_package== 'caret'){

			rmv<-unique(unlist(apply(mxdattr, 2, function(x) which(is.na(x)))))
			if(length(rmv)>0) dat_tr<-dat_tr[-rmv,]
			dat_tr[is.na(dat_tr)]<-0
			rmv<-unique(unlist(apply(mxdattst, 2, function(x) which(is.na(x)))))
			if(length(rmv)>0) dat_tst<-dat_tst[-rmv,]
			dat_tst[is.na(dat_tst)]<-0

			model_type = 'rf'
			if(model_type == 'rf'){
			
				nfolds<- 3 #as.numeric(model_options[2])
				ntrees<- 10 #as.numeric(model_options[3])
				max_depth<- 20 #as.numeric(model_options[4])
				bc<-TRUE #   as.logical(model_options[5])
				# mtries<-3 # as.numeric(model_options[6])
				repeats = 3
				predict_type = 'class'

				if(predict_type == 'class'){
					fitControl <- trainControl(## 10-fold CV
							method = "repeatedcv",
							number = nfolds,
							classProbs = TRUE, 
							repeats = repeats,
							summaryFunction = twoClassSummary)
					
					fit <- train(dat_tr[,input_vars,drop = FALSE], as.character(dat_tr[,opt_var]), 
						method = 'rf',
						metric = 'ROC',
						trControl = fitControl,
						returnData = FALSE,
						ntree = ntrees)

					fit$finalModel<-purge(fit$finalModel)

				}else{
					dat_tr[,opt_var]<-as.numeric(dat_tr[,opt_var])
					dat_tst[,opt_var]<-as.numeric(dat_tst[,opt_var])

					fitControl <- trainControl(## 10-fold CV
						method = "repeatedcv",
						number = nfolds,
						returnData = FALSE,
						repeats = repeats)
					
					fit <- train(dat_tr[,input_vars, drop = FALSE], as.numeric(dat_tr[,opt_var]), 
						method = 'rf',
						trControl = fitControl,
						ntree = ntrees)

					fit$finalModel<-purge(fit$finalModel)

				}
			
				
				
			}else if(model_type == 'dl'){
				# nfolds<-as.numeric(model_options[2])
				# activation<-(model_options[3])
				# loss<-(model_options[4])
				# bc<-as.logical(model_options[5])
				# n_layers<-as.numeric(model_options[6])
				# n_nodes<-as.numeric(model_options[7])

				# hidden <- rep(n_nodes, n_layers)

				# fit <- h2o.deeplearning(x = input_vars, y = opt_var,
				# 	activation = activation,
				# 	hidden = hidden,
				# 	nfolds = nfolds,
				# 	loss = loss,
				# 	balance_classes = bc,
				# 	training_frame = dat_h2o_tr)

						
			}else if(model_type == 'tf'){
				# nfolds<-as.numeric(model_options[2])

				# fit <- h2o.naiveBayes(x = input_vars, y = opt_var,
				# 	training_frame = dat_h2o_tr, laplace = 3)

			}else if(model_type == 'nb'){
				# nfolds<-as.numeric(model_options[2])

				# fit <- h2o.naiveBayes(x = input_vars, y = opt_var,
				# 	training_frame = dat_h2o_tr, laplace = 3)

			}else if(model_type == 'gbm'){
				# nfolds<-as.numeric(model_options[2])

				# fit <- h2o.gbm(x = input_vars, y = opt_var,
				# 	nfolds = nfolds,
				# 	training_frame = dat_h2o_tr)
			}else if(model_type == 'lm'){
				nfolds<-as.numeric(model_options[2])
				repeats = 3
				dat_tr[,opt_var]<-as.numeric(dat_tr[,opt_var])
				dat_tst[,opt_var]<-as.numeric(dat_tst[,opt_var])

			
				fitControl <- trainControl(## 10-fold CV
					method = "repeatedcv",
					number = nfolds,
					returnData = FALSE,
					repeats = repeats)
				
				
				fit <- train(dat_tr[,input_vars, drop = FALSE], as.numeric(dat_tr[,opt_var]), 
					method = 'glm',
					model = FALSE,
					trControl = fitControl)


				fit$finalModel<-purge(fit$finalModel)
			}
			
			

		}
		

	
		dtts<-as.Date(dat_tst$tr_date)
		exdtts<-as.Date(dat_tst$ex_date)
		nms<-names(dat_tst)
			
			

			
		keep.nms<-nms[grepl('tr_date|ticker|cat|fut_|div_value|earnings|trd_size|split|pca|raw|buy_sell|ret|shp|buy_sell', nms)]
		keep.nms<-unique(c(keep.nms, input_vars, ret_var))

			
		if(predict_type == 'regr' | model_type == 'lm'){
			dd<-as.data.frame(dat_tst[,keep.nms])
			pp <- predict(fit, dat_tst)
			
			
			pred<-data.frame(predict = 'SELL', regr = unlist(pp))
			pred$predict[pred$regr>0]<-'BUY'
					
			dd<-data.frame(dd, pred)

			predictions<-pred$predict
		}else{

			dd<-as.data.frame(dat_tst[,keep.nms])
			pp <- predict(fit,newdata = dat_tst, type = "prob")
			predict <- predict(fit,newdata = dat_tst, na.action = NULL,type = "raw")
			
			pred<-data.frame(predict, pp)

			predictions<-as.data.frame(pred$predict)
			
			dd<-data.frame(dd, pred)

			# pred$buy_sell<-dd$buy_sell
		}


	
		

		wgt<-wgts
		wgt<-data.frame(ticker = names(wgt), weight = t(wgt))
		
		dd$test_year<-tst_yr
		dd<-merge(dd, wgt, by = 'ticker',all.x = TRUE)
			
		res<-list(fit, dd)  

		
		res_yrs[[q]]<-res

		# if(ticker_group == 'cluster'){
		# 	print('Shutdown h2o')
		# 	tryCatch(h2o.shutdown(prompt = FALSE), error = function(err){return(NULL)})
		# 	Sys.sleep(5)
		# 	print("Shutdown Success")
		# }
	
	} ## end test_years loop

	# fut_fit<-res_yrs[[which(test_years == 0)]]
	# res_yrs[which(test_years == 0)]<-NULL


	# if(is.null(cl)){
	# 	num_workers<-detectCores()-1		
	# 	# cl<-makeCluster(num_workers) #, outfile = '/temp/RCluster.log')
	# 	cl<-makeCluster(num_workers)
	# 	clusterEvalQ(cl,source("code/DaVinci/R/src/tim/TimTest.R"))
	# }


	pred<-lapply(res_yrs, function(x) {
		return(data.frame(x[[2]]))
	})



	dd<-data.frame(rbindlist(pred))

	ncut = 10
	remv<-unique(unlist(lapply(input_vars, function(x,dd) which(is.na(dd[,x])), dd)))
	if(length(remv)>0) dd<-dd[-remv,]

	if(predict_type == 'regr'){
		
		dd$prediction<-dd$predict
			# dd$buy_trade_cut_range<-cut(as.numeric(dd$regr) , breaks=10, include.lowest=TRUE)
		dd$buy_trade_cut_range<-cut(as.numeric(dd$regr) ,ncut,  include.lowest=TRUE)

		dd$buy_trade_cut<-as.numeric(dd$buy_trade_cut_range)
	
	}else if(predict_type == 'class'){

		
		dd$prediction<-dd$predict
				# dd$buy_trade_cut_range<-cut(as.numeric(dd$BUY) , breaks=ncut,  include.lowest=TRUE)
		# dd$buy_trade_cut_range<-cut(as.numeric(dd$BUY) ,unique(quantile(as.numeric(dd$BUY), probs=0:ncut/ncut)),  include.lowest=TRUE)
		dd$buy_trade_cut_range<-cut(as.numeric(dd$BUY) ,ncut,  include.lowest=TRUE)
# 
		dd$buy_trade_cut<-as.numeric(dd$buy_trade_cut_range)
	

	}
	# pred$predict<-'SELL'
		# pred$predict[pred$BUY>.5]<-'BUY'
		# pred$predict[pred$SELL>.5]<-'SELL'
	
	labels <- dd$buy_sell
	dd$test_year<-as.numeric(format(as.Date(dd$tr_date), "%Y"))

	# labels <-as.data.frame(dat_h2o_tst[,opt_var])[,1]

	test_auc<-cvAUC::AUC(predictions = as.numeric(as.factor(dd$prediction)), labels = as.numeric(dd$buy_sell))
	

	err.res<-confusionMatrix(as.character(dd$prediction) , as.character(dd$buy_sell))
	print(err.res)
	Sys.sleep(.5)

		
	flush.console()
	dd$correct<-1
# pred_var<-paste('predict_',nday_ret, sep = "")
	pred_var<-'prediction'
	# dd[,pred_var]<-factor(dd[,pred_var], levels = c("BUY", "SELL"))
	
	dd$correct[as.character(dd$buy_sell) != as.character(dd[,pred_var])] <- 0
	
	
	if(type == 'yh_spot'){
		# ret_var<-"stk_ret_2"
		retvar<-as.numeric(dd[,ret_var])
		dd$trade_return<-0
		# dd$trade_return[dd$correct ==1 & dd[,paste('predict_',n, sep = "")] == 'TRADE']<-abs(retvar[dd$correct ==1 & dd[,paste('predict_',n, sep = "")] == 'TRADE'])
		# dd$trade_return[dd$correct ==0 & dd[,paste('predict_',n, sep = "")] == 'TRADE']<- -1 *abs(retvar[dd$correct ==0 & dd[,paste('predict_',n, sep = "")] == 'TRADE'])
		dd$trade_return[dd$correct ==1]<-(retvar[dd$correct ==1])
		dd$trade_return[dd$correct ==0]<- -1 *(retvar[dd$correct ==0])

		dd$gross_profit<-dd$trade_return * 1000
		dd$share_size <-0
		dd$share_size<-ceiling(1000/dd$spot_raw)
	
		dd$spot_share_fee <- -1 * abs(sign(dd$share_size))*(pmax(1, pmin(abs(dd$share_size)*.005, abs(dd$share_size)*dd$spot_raw*.005)) + .02 * abs(dd$share_size))
		dd$spot_share_spread <- -1 * .02 * abs(dd$share_size)
		dd$net_profit<-dd$gross_profit + dd$spot_share_fee + dd$spot_share_spread



	}else{
		ret_df<-(dd[,c(pred_var,'correct','buy_sell',ret_var)])
		ret_df[ret_df[,pred_var] == 'SELL', ret_var]<- -1 * ret_df[ret_df[,pred_var] == 'SELL', ret_var]
		ret_df$trade_return<-ret_df[,ret_var]

		dd$gross_profit<-ret_df$trade_return
		dd$net_profit<-dd$gross_profit #+ dd$spot_share_fee + dd$spot_share_spread
		dd$net_profit_contract<-dd$net_profit/dd$adj_trd_size
	}
		
	
# lvls<-unique(pred$BUY_TRADE_cut)
	# lvls<-c(0, .1, .2, .3, .4, .5, .6, .7, .8, .9)
	lvls<-c(1:10)
	# lvls<-lvls[order(lvls)]

	# lvl.dat<-calc_level_data(dd, lvls = lvls, predict_type = predict_type,ret_var = "gross_profit")
	lvl.dat<-calc_level_data_years(dd, lvls = lvls, predict_type = predict_type,ret_var = "gross_profit")
	# lvl.dat2<-calc_level_data_years(dd, lvls = lvls, predict_type = predict_type,ret_var = "net_profit_contract")


	# res_file_dat<-data.frame(model_id = model_id, mdl_type = model_type,model_package=model_package, predict_type=predict_type,ticker_domain = ticker_domain,ticker_group = ticker_group, ticker = ticker,ret_var = ret_var,  opt_var = opt_var, namet = namet, cluster = cluster, org_dat )  # ntrees = ntrees, max_depth_limit = max_depth, balance_classes = bc,
	
	
	###########################################

	fit<-res_yrs[[length(res_yrs)]][[1]]
	
	if(model_type == 'rf'){

		
		gg<-data.frame(randomForest::importance(fit$finalModel))
		names(gg)<-"value"
		gg$predict_type<-predict_type
		gg$input_var<-unlist(rownames(gg))
		gg$imp_ratio<-gg[,1]/sum(gg[,1], na.rm = TRUE)
		gg$imp_rank<-rank(-gg$imp_ratio)
		# imp.vars<-data.frame(model_id = model_id, ret_var = ret_var, gg)
		imp.vars<-data.frame(ret_var = ret_var, gg)

		imp.vars$n_vars<-nrow(imp.vars)
		
		results<-fit$results
		rmse<-results$RMSE
		rsquared<-results$Rsquared
		
		fit<-fit$finalModel
	
	}

	if(model_type == 'lm'){
					
		# fit<-fut_fit[[1]][[1]]

		imp.vars<-data.frame(model_id = model_id, ret_var = ret_var, variable = rownames(data.frame(fit$finalModel$coefficients)),coef = fit$finalModel$coefficients)
		imp.vars$n_vars<-nrow(imp.vars)
		gg<-imp.vars
	
		nn<-aggregate(list(gg$coef, gg$n_vars), by = list(gg$variable), mean, na.rm = TRUE)
		names(nn)<-c('variable','coef','n_vars')
		vv<-data.frame(model_id = gg$model_id[1], ret_var =gg$ret_var[1], nn)
		gg<-vv
		imp.vars<-gg
		
		results<-fit$results
		rmse<-results$RMSE
		rsquared<-results$Rsquared

		fit<-fit$finalModel
		

	}
	
	check_make_directory(loc.fldr, fldr = TRUE)
	loc.file<-paste0(loc.fldr,"/", model_id, '_fit.rds')
	saveRDS(fit, file = loc.file)




	
}
