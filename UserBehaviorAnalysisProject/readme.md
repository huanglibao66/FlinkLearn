

## NetWorkFlowAnalysis

### NetWorkFLowAnalysis
**每隔5秒，输出最近10分钟内访问量最多的前N个URL**

### PageView
**统计pv**

### UniqueVisitor
**统计uv**

### UvWithBloom
**使用布隆过滤器对uv进行去重**
**使用redis自定义布隆过滤器**


## MarketAnalysis
**市场营销商业指标分析**


### AppMarketingByChannel
**根据来源方式**</br>
**自定义数据源**

### AppMarketing
**总体计数**

### AdStatisticsByGeo
**根据地理位置进行统计**
**添加黑名单，防止刷单**
**对添加到黑名单的id进行定时，到期解除**


## LoginFailDetect
**登陆失败次数检查，防止恶意登录**

### LoginFail
**使用状态编程来完成业务需求**

### LoginFailWithCep
**使用CEP包进行处理**


## OrderPayDetect
**订单支付实时监控**

### OrderTimeOut
**使用CEP实现了超时报警的需求**
**设置支付失效事件**

### OrderTimeoutWithoutCep
**订单支付失效状态编程**

### TxMacthDetect
**实时对账**
**同事处理两条流**

