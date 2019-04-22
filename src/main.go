package main

import (
	"core"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)
var config core.Config

func main(){

	//读取配置文件
	configPath := "./config.conf"
	config,err := core.ReadConfig(configPath)
	if err!=nil{
		core.Logger(err.Error())
	}

	//查询api
	//go func(){
	//	handle := http.HandlerFunc(netHandle)
	//	http.Handle("/CheckIsNewUser",handle)
	//	err = http.ListenAndServe(config.Address,nil)
	//	if err!=nil{
	//		core.Logger(err.Error())
	//	}
	//}()



	//对于固定时间的定时器，可以用sleep，到了时间才启动
	fixTime, err := core.GetFixTime(&config)
	if err != nil {
		core.Logger("获取定时器固定时间出错！")
		return
	}
	//定时更新日数据
	Timerwork()
	timer := time.NewTicker(time.Hour*3)
	for {
		select {
		case <-timer.C:
			//设置时间
			local,_:=time.LoadLocation("Local")
			nowTime:=time.Now()
			toFixTimeStr:=nowTime.Format("2006")+"-"+nowTime.Format("01") +"-"+nowTime.Format("02")+" "+fixTime.Format("15")+":"+fixTime.Format("04")+":"+fixTime.Format("05")
			toFixTime,_:=time.ParseInLocation("2006-01-02 15:04:05",toFixTimeStr,local)
			for toFixTime.After(time.Now()){
				time.Sleep(time.Second * 1)
			}
			Timerwork()
		}
	}
}

func netHandle(w http.ResponseWriter,req *http.Request){
	//读取配置文件
	configPath := "./config.conf"
	config,err := core.ReadConfig(configPath)
	if err!=nil{
		core.Logger(err.Error())
		w.Write([]byte("500"))
		return
	}
	auth := req.Header.Get("Authorization")
	if auth == "" {
		w.Header().Set("WWW-Authenticate", `Basic realm="Unauthorized"`)
		w.WriteHeader(http.StatusUnauthorized)
		core.Logger("error auth")
		return
	}
	auths := strings.SplitN(auth," ",2)
	if len(auths)!=2{
		w.Write([]byte("Error Arguments"))
		core.Logger("error Arguments")
		return
	}
	authMethod:=auths[0]
	authMd5 := auths[1]
	switch authMethod {
	case "Basic":
		jsonObj := make(map[string]interface{})
		authstr := string(authMd5)
		//authstr, err := base64.StdEncoding.DecodeString(authMd5)
		//if err != nil {
		//	core.Logger(err.Error())
		//	w.Write([]byte("Unauthorized!"))
		//	return
		//}
		userKeyMd5 := strings.ToLower(core.MD532(config.ApiKey))
		if authstr != userKeyMd5{
			core.Logger("Error Auth")
			jsonObj["ReturnCode"] = 401
			jsonObj["ReturnMsg"] = "认证出错"
			bytesData, _ := json.Marshal(jsonObj)
			if err != nil {
				core.Logger(err.Error())
				w.Write([]byte("500"))
				return
			}
			w.Write(bytesData)
			return
		}
		phone := &core.Phone{}
		if err != nil {
			core.Logger(err.Error())
			return
		}
		body,err:= ioutil.ReadAll(req.Body)
		if err != nil {
			core.Logger(err.Error())
			jsonObj["ReturnCode"] = 400
			jsonObj["ReturnMsg"] = "请求出错"
			bytesData, _ := json.Marshal(jsonObj)
			if err != nil {
				core.Logger(err.Error())
				w.Write([]byte("500"))
				return
			}
			w.Write(bytesData)
			return
		}
		err = json.Unmarshal(body,phone)
		if err != nil {
			core.Logger(err.Error())
			w.Write([]byte("500"))
			return
		}
		if len(phone.Mobile)!=11{
			core.Logger("Error phone number")
			w.Write([]byte("Error phone number"))
			return
		}
		bool,err:= core.CheckIsNewUserByRedisAB(phone.Mobile,config)
		if err != nil {
			core.Logger(err.Error())
			jsonObj["ReturnCode"] = 500
			jsonObj["ReturnMsg"] = "查询出错"
			bytesData, _ := json.Marshal(jsonObj)
			if err != nil {
				core.Logger(err.Error())
				w.Write([]byte("500"))
				return
			}
			w.Write(bytesData)
			return
		}
		jsonObj["Mobile"] = phone.Mobile
		jsonObj["IsPeNewUser"] = bool
		jsonObj["ReturnCode"] = 200
		jsonObj["ReturnMsg"] = "成功"
		bytesData, err := json.Marshal(jsonObj)
		if err != nil {
			core.Logger(err.Error())
			w.Write([]byte("500"))
			return
		}
		w.Write(bytesData)
	default:
		core.Logger("Not Basic Method")
		w.Write([]byte("Not Basic Method"))
	}
}

//日更新操作
func DayUpdate(config core.Config,users []*core.TPeUser) error{
	//去重
	//daySql:="select * from t_pe_dayuser a where NOT EXISTS (select 1 from t_pe_dayuser b WHERE a.phone = b.phone and a.logintime < b.logintime) GROUP by a.phone"
	//dayUsers := []core.TPeDayuser{}
	//err := db.SQL(daySql).Find(&dayUsers)
	//if err!=nil{
	//	return err
	//}
	db := core.CreatEngine(config)
	//先删除全部数据
	err:=core.DeletePETable(db)
	if err!=nil{
		core.Logger(err.Error())
		return err
	}
	selectNums:= int(math.Ceil(float64(len(users))/500000))
	quit := make(chan int)
	if selectNums==1{
		err = core.InsertPeUser(db,users)
		if err!=nil{
			core.Logger(err.Error())
			return err
		}
	}else{
		core.Logger("共开启"+strconv.Itoa(selectNums)+"个协程")
		for i:=0;i<selectNums;i++{
			nums := users[i*500000:(i+1)*500000]
			if i==selectNums-1{
				nums = users[i*500000:]
			}
			go core.AsyncInsertPeUser(db,nums,i,quit)
		}
		for i:=0;i<selectNums;i++{
			<-quit
		}
		core.Logger("数据库插入完毕")
	}
	return nil
}


func Timerwork() {
	//读取配置文件
	configPath := "./config.conf"
	config,err := core.ReadConfig(configPath)
	if err!=nil{
		core.Logger(err.Error())
	}
	csvUtil := &core.CsvUtil{}
	dateStr := time.Now().Add(-time.Hour*24).Format("20060102")
	fmt.Println("dateStr:" + dateStr)
	b, err := csvUtil.IsExist(dateStr)
	//c, err := csvUtil.IsExist("processing")
	if err != nil {
		core.Logger("csv error")
		return
	}
	if b {
		return
	}else{
		//获取文件
		path,err := core.FtpGet(&config,dateStr)
		if err!=nil{
			core.Logger("获取文件出错：" + err.Error())
			return
		}
		core.Logger("获取文件成功，文件地址："+path)
		//解压文件
		//err = core.UnCompressFile("./files/pushemail2001_20190416.tar.gz")

		err = core.UnCompressFile(path)
		if err!=nil{
			core.Logger("解压文件出错：" + err.Error())
			return
		}
		//解析文件
		analyPath:= "a_PushEmail2001_"+ dateStr +"_001.dat"
		users,numsStr,err := core.AnalysisText(analyPath)
		if err!=nil{
			core.Logger("解析文件出错：" + err.Error())
			return
		}
		//先更新数据库
		err = DayUpdate(config,users)
		if err!=nil{
			core.Logger(err.Error())
			return
		}
		//再更新redisZero
		client,err:=core.NewClientZero(config)
		if err!=nil{
			core.Logger("连接redis出错" + err.Error())
			return
		}
		err = core.PeUsersInsertRedis(client,numsStr)
		if err!=nil{
			core.Logger("更新redisZero出错" + err.Error())
			return
		}
		core.Logger("更新redisZero成功")
		//再更新redisOne
		//time.Sleep(time.Minute*5)
		clientOne,err:=core.NewClientOneByDefaultClient(client)
		if err!=nil{
			core.Logger("连接redis出错" + err.Error())
			return
		}
		err = core.PeUsersInsertRedis(clientOne,numsStr)
		if err!=nil{
			core.Logger("更新redisOne出错" + err.Error())
			return
		}
		core.Logger("更新redisOne成功")
	}
	//最后所有操作成功后将文件日期名记录
	csvUtil.Put(dateStr)
	//删除文件
	datFile := "./files/a_PushEmail2001_"+ dateStr +"*"
	tarGzFile := "./files/pushemail2001_"+dateStr + ".tar.gz"
	core.RemoveFiles(datFile)
	core.RemoveFiles(tarGzFile)
}

