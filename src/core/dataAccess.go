package core

import (
	_ "github.com/go-sql-driver/mysql"
	"github.com/go-xorm/xorm"
	"math"
	"strconv"
)

func CreatEngine(config Config) *xorm.Engine{
	dataSourceName:=config.MysqlDataSource
	//整体格式:"数据库用户名:密码@(数据库地址:3306)/数据库实例名称?charset=utf8"
	//MysqlDataSource = root:root@(192.168.14.178:3306)/test?charset=gbk
	engine,err := xorm.NewEngine("mysql",dataSourceName)
	if err!=nil{
		Logger(err.Error())
	}
	//连接测试
	//if err := engine.Ping(); err!=nil{
	//	Logger(err.Error())
	//	return nil
	//}
	return engine
}

//更新每日数据操作
func UpdateDayUsers(db *xorm.Engine,peUsers []TPeUser,dayPhones []int64) error {
	delUsers := []TPeUser{}
	delPhones := []int64{}
	//先查已经存在的数据
	db.In("phone",&dayPhones).Find(&delUsers)
	for _,delUser:=range delUsers{
		delPhones = append(delPhones, delUser.Phone)
	}
	//删除数据
	delUser := &TPeUser{}
	_,err:=db.In("phone",delPhones).Delete(delUser)
	if err!=nil{
		return err
	}
	//批量插入数据
	_,err =db.Insert(peUsers)
	if err!=nil{
		return err
	}
	return nil
}

//并发更新每日数据操作
func AsyncUpdateDayUsers(db *xorm.Engine,peUsers []TPeUser,dayPhones []int64,i int, quit chan int) {
	defer func() {
		quit <- i
		Logger("协程"+strconv.Itoa(i)+"：END")
	}()
	Logger("协程"+strconv.Itoa(i)+"：START")
	err := UpdateDayUsers(db,peUsers,dayPhones)
	if err!=nil{
		Logger(err.Error())
	}
}

//删除全部数据
func DeletePETable(db *xorm.Engine) error{
	sql := "truncate table t_pe_user"
	_,err := db.Exec(sql)
	return err
}
//插入数据
func InsertPeUser(db *xorm.Engine, users []*TPeUser) error{
	_,err:=db.Insert(&users)
	return err
}
//并发插入数据
func AsyncInsertPeUser(db *xorm.Engine, users []*TPeUser,i int, quit chan int){
	defer func() {
		quit <- i
		Logger("协程"+strconv.Itoa(i)+"：END")
	}()
	Logger("协程"+strconv.Itoa(i)+"：START")
	nums := int(math.Ceil(float64(len(users))/5000))
	for j:=0;j<nums;j++{
		peUsers := []*TPeUser{}
		if j==nums-1{
			peUsers = users[j*5000:]
		}else{
			peUsers = users[j*5000:(j+1)*5000]
		}
		err := InsertPeUser(db,peUsers)
		if err!=nil{
			Logger(err.Error())
		}
	}
	//if len(users)==500000{
	//	for j:=0;j<100;j++{
	//		//Logger("第"+ strconv.Itoa(i) +","+ strconv.Itoa(j)+"组start")
	//		peUsers := users[j*5000:(j+1)*5000]
	//		err := InsertPeUser(db,peUsers)
	//		if err!=nil{
	//			Logger(err.Error())
	//		}
	//		//Logger("第"+ strconv.Itoa(i) +","+ strconv.Itoa(j)+"组end")
	//	}
	//}else {
	//	nums := int(math.Ceil(float64(len(users))/5000))
	//	for j:=0;j<nums;j++{
	//		peUsers := []*TPeUser{}
	//		if j==nums-1{
	//			peUsers = users[j*5000:]
	//		}else{
	//			peUsers = users[j*5000:(j+1)*5000]
	//		}
	//		//Logger("第"+ strconv.Itoa(i) +","+ strconv.Itoa(j)+"组start")
	//		err := InsertPeUser(db,peUsers)
	//		if err!=nil{
	//			Logger(err.Error())
	//		}
	//		//Logger("第"+ strconv.Itoa(i) +","+ strconv.Itoa(j)+"组end")
	//	}
	//}
}

//查询是否pe新人数据(直接查数据库)
func CheckIsNewUser(phone string,config Config)(int,error) {
	db := CreatEngine(config)
	//number,err:=strconv.ParseInt(phone,10,64)
	//if err!=nil  {
	//	return 0,err
	//}
	peUsers:=[] *TPeUser{}
	err := db.Where("phone =?",phone).Find(&peUsers)
	if err!=nil  {
		return 0,err
	}
	//表中有数据，返回0；无则返回1
	if len(peUsers)==0 {
		return 1,nil
	}else{
		return 0,nil
	}
}

//查询是否pe新人数据(先查缓存，再查数据库)
func CheckIsNewUserByRedis(phone string,config Config)(int,error) {
	client,err:=NewClientZero(config)
	if err!=nil{
		return 0,err
	}
	re,err:=client.Exists(phone).Result()
	if err!=nil{
		return 0,err
	}
	bool := 0
	if int(re)==0{
		bool = 1
	}
	return bool,nil
}

//根据redisAB两库一起查询
func CheckIsNewUserByRedisAB(phone string,config Config)(int,error) {
	client,err:=NewClientZero(config)
	if err!=nil{
		return 0,err
	}
	re,err:=client.Exists(phone).Result()
	if err!=nil{
		return 0,err
	}
	clientB,err:=NewClientOneByDefaultClient(client)
	if err!=nil{
		return 0,err
	}
	reb,err:=clientB.Exists(phone).Result()
	if err!=nil{
		return 0,err
	}
	bool := 0
	if int(re)==0 && int(reb)==0{
		bool = 1
	}
	return bool,nil
}

func CheckIsNewUserByRedisAB2(phone string,config Config)(int,error) {
	bool := 0
	client,errA:=NewClientZero(config)
	if errA!=nil{
		//0库崩了，查1库
		clientB,errB:=NewClientOne(config)
		if errB!=nil{
			return 0,errB
		}
		reb,err:=clientB.Exists(phone).Result()
		if err!=nil{
			return 0,err
		}
		if int(reb)==0{
			bool = 1
		}
		return bool,nil
	}
	reA,errA:=client.Exists(phone).Result()
	if errA!=nil{
		//0库崩了，查1库
		clientB,errB:=NewClientOne(config)
		if errB!=nil{
			return 0,errB
		}
		reb,err:=clientB.Exists(phone).Result()
		if err!=nil{
			return 0,err
		}
		if int(reb)==0{
			bool = 1
		}
		return bool,nil
	}
	//0库没崩，以0库为准
	if int(reA)==0{
		bool = 1
	}
	return bool,nil
}