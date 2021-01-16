# datadown-tiantianfund
通过天天基金网数据接口下载/更新基金数据，并经过数据清洗进入到mysql中

主要使用的接口地址：

1、获取市场上基金名称和类型：http://fund.eastmoney.com/js/fundcode_search.js

2、获取基金详细数据：http://fund.eastmoney.com/pingzhongdata/000001.js


### 确定环境中存在以下库
```
pip install -r requirements
```

### 确定mysql已经安装

### 生成fund_list数据表
初次运行需要在生成fund_list数据表，方便之后根据基金类型或基金代码下载
```
python fund_list_down.py --account root --password 123456 --host localhost --post 3306 --database fund
```
--account mysql账号名称  --password mysql密码  --host mysql地址  --post mysql端口  --database mysql数据库名称

基金类型与数字代号对应关系见fund-type.xlsx

### 生成fund_data_all数据表
初次运行必须是update==False，下载全部基金数据
```
python fund_data_down.py --account root --password 123456 --host localhost --post 3306 --database fund --update False
```

update==True 则进入更新模式，更新fund_data_all中的数据
```
python fund_data_down.py --account root --password 123456 --host localhost --post 3306 --database fund --update True
```
