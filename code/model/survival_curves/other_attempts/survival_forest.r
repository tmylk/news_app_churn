# Random forest that splits on how different survival curves are
# It is closer to business value than MSE
# ran for 4 days on 10 cores and didn't finish but memory grew consistently

library("RandomForestSRC")
indo=read.csv('/root/data.csv')
v.obj <- rfsrc(Surv(ll_duration,ll_event)~., data=indo)
print(vimp(airq.obj)$importance)