package core

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/jlaffaye/ftp"
	"gopkg.in/ini.v1"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	//配置文件要通过tag来指定配置文件中的名称
	//api地址
	Address 			 string `ini:"Address"`
	//from-sftp
	FromFtpHost          string `ini:"FromFtpHost"`
	FromFtpLoginUser     string `ini:"FromFtpLoginUser"`
	FromFtpLoginPassword string `ini:"FromFtpLoginPassword"`
	//MySql
	MysqlDataSource string `ini:"MysqlDataSource"`
	//
	FixedTime string `ini:"FixedTime"`
	//Api，auth
	ApiKey string `ini:"ApiKey"`
	//Redis
	RedisIP string `ini:"RedisIP"`
	RedisPwd	string `ini:"RedisPwd"`
}

func Logger(strContent string) {
	logPath := "./log/" + time.Now().Format("2006-01-02") + ".txt"
	file, _ := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0777)
	fileTime := time.Now().Format("2006-01-02 15:04:05")
	fileContent := strings.Join([]string{"===", fileTime, "===", strContent, "\n"}, "")
	buf := []byte(fileContent)
	file.Write(buf)
	defer file.Close()
}


func ReadLog(path string,rex string,rexls string) string {
	file,_:=os.Open(path)
	defer file.Close()
	bye,err:=ioutil.ReadAll(file)
	if err!=nil{
		log.Panic(err)
	}
	str := string(bye[:])
	index:=strings.Index(str,rex)
	index2:=strings.Index(str,rexls)
	if index>0{
		selected:=str[index:index2]
		fmt.Println(selected)
		return selected
	}
	return ""
}


//读取配置文件并转成结构体
func ReadConfig(path string) (Config, error) {
	var config Config
	conf, err := ini.Load(path) //加载配置文件
	if err != nil {
		Logger("load config file fail!")
		return config, err
	}
	conf.BlockMode = false
	err = conf.MapTo(&config) //解析成结构体
	if err != nil {
		Logger("mapto config file fail!")
		return config, err
	}
	return config, nil
}

func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

//获取目录
func getDir(path string) string {
	return subString(path, 0, strings.LastIndex(path, "/"))
}

//截取字符串,截取的不包括第end位
func subString(str string, start, end int) string {
	rs := []rune(str)
	length := len(rs)
	if start < 0 || start > length {
		panic("start is wrong")
	}
	if end < start || end > length {
		panic("end is wrong")
	}
	return string(rs[start:end])
}

//3DES加密
//func Zip3DESEncrypt(zipPath string, key string, cbc *CbcDesEncrypt) error {
//	logzip, _ := os.OpenFile(zipPath, os.O_RDWR, 0777)
//	defer logzip.Close()
//	buff, err := ioutil.ReadAll(logzip)
//	if err != nil {
//		Logger("jiami")
//		return err
//	}
//	keyBytes := []byte(key)
//	encryptBuff := cbc.Encrypt3DES(buff, keyBytes)
//	logzipdes, err := os.Create(zipPath + ".des")
//	if err != nil {
//		return err
//	}
//	defer logzipdes.Close()
//	_, err = logzipdes.Write(encryptBuff)
//	if err != nil {
//		return err
//	}
//	return nil
//}

//3DES解密
//func Zip3DESDEncrypt(zipDesPath string, key string, cbc *CbcDesEncrypt) error {
//	logzipdes, _ := os.OpenFile(zipDesPath, os.O_RDWR, 0777)
//	defer logzipdes.Close()
//	buff, err := ioutil.ReadAll(logzipdes)
//	if err != nil {
//		return err
//	}
//	keyBytes := []byte(key)
//	dencryptBuff := cbc.Decrypt3DES(buff, keyBytes)
//	toPath := subString(zipDesPath, 0, len(zipDesPath)-8) + "2.zip"
//	logzip2, err := os.Create(toPath)
//	if err != nil {
//		return err
//	}
//	defer logzip2.Close()
//	_, err = logzip2.Write(dencryptBuff)
//	if err != nil {
//		return err
//	}
//	return nil
//}

//生成32位MD5
func MD532(text string) string {
	ctx := md5.New()
	ctx.Write([]byte(text))
	return hex.EncodeToString(ctx.Sum(nil))
}

//用linux自带的openssl加密3DES-CBC,command的首参是openssl,不是平常的/bin/bash
func Encrypt3DESByOpenssl(key string, fileName string) (desPath string, err error) {
	filePath := "./formatFiles/" + fileName
	desPath = filePath + ".des"
	fmt.Println("将要加密的文件地址：" + filePath)
	cmd := exec.Command("openssl", "enc", "-des-ede3-cbc", "-e", "-k", key, "-in", filePath, "-out", desPath)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		Logger("Error:can not obtain stdout pipe for command")
		return "", err
	}
	//执行命令
	if err := cmd.Start(); err != nil {
		Logger("Error:The command is err")
		return "", err
	}
	//读取所有输出
	_, err = ioutil.ReadAll(stdout)
	if err != nil {
		return "", err
	}
	if err := cmd.Wait(); err != nil {
		Logger("wait error")
		return "", err
	}
	Logger("encrypt success:")
	//fmt.Printf("stdout:\n\n %s", "")
	return desPath, nil
}


//重构文本-模板
//func FormatJKText(kd *KdcheckResult) string {
//	accounttype := ""
//	if kd.accounttype == "1" {
//		accounttype = "手机宽带"
//	} else if kd.accounttype == "2" {
//		accounttype = "裸宽宽带"
//	} else {
//		accounttype = "其他宽带"
//	}
//	isYearPackAge := "否"
//	if kd.IsYearPackAge == "1" {
//		isYearPackAge = "是"
//	}
//	//str := "START|" + kd.KdAccount + "|\n" + "宽带属性|" + accounttype + "~家庭宽带~" + kd.UserStatus + "~" + isYearPackAge + "~" + kd.LastDate + "~" + kd.BroadSpeed + "|010000\nEND\n"
//	//脱敏
//	//phoneNum:=subString(kd.KdAccount,0,3)+"****"+subString(kd.KdAccount,7,11)
//	str := "START|" + kd.KdAccount + "|\n" + "宽带属性|" + kd.KdAccount + "~" + accounttype + "~" + kd.UserStatus + "~" + isYearPackAge + "~" + kd.LastDate + "~" + kd.BroadSpeed + "|010000\nEND\n"
//	//utf8->gbk
//	str = Encode(str)
//	return str
//}

//tar命令解压
func UnCompressFile(formatFilePath string) error {
	Logger("uncompressFile Path:"+formatFilePath)
	cmd := exec.Command("tar","-xzvf", formatFilePath,"-C","./files")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		Logger("UnCompressFile Error:can not obtain stdout pipe for command")
		return err
	}
	//执行命令
	if err := cmd.Start(); err != nil {
		Logger("UnCompressFile Error:The command is err")
		return err
	}
	//读取所有输出
	_, err = ioutil.ReadAll(stdout)
	if err != nil {
		return err
	}
	if err := cmd.Wait(); err != nil {
		Logger("UnCompressFile wait error")
		fmt.Println(err)
		return err
	}
	Logger("UnCompressFile success:" + formatFilePath )
	return nil
}

func TarFile(formatFilePath string) error{
	Logger("uncompressFile Path:"+formatFilePath)
	cmd := exec.Command("sh","./tar.sh", formatFilePath)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		Logger("UnCompressFile Error:can not obtain stdout pipe for command")
		return err
	}
	//执行命令
	if err := cmd.Start(); err != nil {
		Logger("UnCompressFile Error:The command is err")
		return err
	}
	//读取所有输出
	str, err := ioutil.ReadAll(stdout)
	mes := string(str)
	if err != nil {
		return err
	}
	if err := cmd.Wait(); err != nil {
		if strings.Contains(mes, "success") {
			Logger("UnCompressFile ftp file success:"+formatFilePath)
			return nil
		}
		Logger("wait error:" + string(str) + ";")
		return err
	}
	Logger("UnCompressFile success:" + formatFilePath )
	return nil
}

//删除文件
func RemoveFiles(path string)error{
	Logger("RemoveFile Path:" + path)
	cmd := exec.Command("sh","./rm.sh", path)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		Logger("RemoveFiles Error:can not obtain stdout pipe for command")
		return err
	}
	//执行命令
	if err := cmd.Start(); err != nil {
		Logger("RemoveFiles Error:The command is err")
		return err
	}
	//读取所有输出
	_, err = ioutil.ReadAll(stdout)
	if err != nil {
		return err
	}
	if err := cmd.Wait(); err != nil {
		Logger("RemoveFiles wait error")
		fmt.Println(err)
		return err
	}
	Logger("RemoveFile success:" + path)
	return nil
}

//取定时时间
func GetFixTime(config *Config) (fixTime time.Time, err error) {
	fixTimeStr := config.FixedTime
	//fixTime := time.Date(2018, 11, 06, 07, 52, 0, 0, time.Local)
	year, err := strconv.Atoi(subString(fixTimeStr, 0, 4))
	if err != nil {
		return fixTime, err
	}
	monthNum, _ := strconv.Atoi(subString(fixTimeStr, 4, 6))
	if err != nil {
		return fixTime, err
	}
	day, _ := strconv.Atoi(subString(fixTimeStr, 6, 8))
	if err != nil {
		return fixTime, err
	}
	hour, _ := strconv.Atoi(subString(fixTimeStr, 8, 10))
	if err != nil {
		return fixTime, err
	}
	min, _ := strconv.Atoi(subString(fixTimeStr, 10, 12))
	if err != nil {
		return fixTime, err
	}
	//这个month竟然还是个time.Month类型，奇葩
	month := time.Month(monthNum)
	fixTime = time.Date(year, month, day, hour, min, 0, 0, time.Local)
	return fixTime, nil
}

//FTP-Get操作
func FtpGetFile(config *Config, dateStr string) (path string, err error) {
	//访问ftp服务器
	entry, err := ftp.Connect(config.FromFtpHost)
	defer entry.Quit()
	if err != nil {
		Logger("connect to ftp server error :" + config.FromFtpHost)
		return "", err
	}
	Logger("connect to ftp server success :" + config.FromFtpHost)
	//login
	entry.Login(config.FromFtpLoginUser, config.FromFtpLoginPassword)
	if err != nil {
		Logger("ftp login error, user:" + config.FromFtpLoginUser + ";pass: " + config.FromFtpLoginPassword)
		return "", err
	}
	Logger("ftp login success")
	//更换目录
	err = entry.ChangeDir("./pe/")
	if err != nil {
		Logger("change dir error: " + err.Error())
		return "", err
	}
	//get
	remoteFile := "pushemail2001_"+dateStr + ".tar.gz"
	//remoteFile := "./logfile/10008105/201810/20181008_001.log"
	res, err := entry.Retr(remoteFile)
	defer res.Close()
	if err != nil {
		Logger("get file error :" + remoteFile)
		Logger(err.Error())
		return "", err
	}
	Logger("get file start :" + remoteFile)
	downloadPath := "./files/" + remoteFile
	file, err := os.Create(downloadPath)
	defer file.Close()

	//一次读取多少字节
	buf := make([]byte, 1024*1024)
	for {
		n, err := res.Read(buf)
		file.Write(buf[:n]) //n是成功读取个数
		if err != nil {     //遇到任何错误立即返回，并忽略 EOF 错误信息
			if err == io.EOF {
				Logger("get file end :" + downloadPath)
				break
			}
			Logger(err.Error())
			return "", err
		}
	}
	Logger("get file end " + downloadPath)
	return downloadPath, nil
}

func FtpGet(config *Config, dateStr string) (path string, err error) {
	remoteFile := "pushemail2001_"+dateStr + ".tar.gz"
	downloadPath := "./files/" + remoteFile
	Logger("ftp 开始下载:"+remoteFile)
	//
	cmd := exec.Command("sh", "./ftpget.sh", config.FromFtpHost, config.FromFtpLoginUser, config.FromFtpLoginPassword, remoteFile)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		Logger("ftp stdout error")
		return "",err
	}
	if err := cmd.Start(); err != nil {
		Logger("Error:The command is err")
		return "",err
	}
	//读取所有输出
	str, err := ioutil.ReadAll(stdout)
	if err != nil {
		Logger("Error:The command read is err")
		return "",err
	}
	mes := string(str)
	if err := cmd.Wait(); err != nil {
		if strings.Contains(mes, "success") {
			Logger("get ftp file success:"+downloadPath)
			return downloadPath,nil
		}
		Logger("wait error:" + string(str) + ";")
		return "",err
	}
	Logger("get ftp file success:"+downloadPath)
	return downloadPath,nil
}

//解析文件
func AnalysisText(filePath string) (users []*TPeUser,numsStr []string, err error) {
	filePath = "./files/" + filePath
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0777)
	info, err := os.Stat(filePath)
	Logger(filePath + " file size is " + strconv.FormatInt(info.Size(), 10))
	defer file.Close()
	if err != nil {
		Logger("open file error :" + filePath)
		return nil,nil, err
	}
	//buf := make([]byte, 12)
	bfrd := bufio.NewReader(file)
	users = []*TPeUser{}
	numsStr = [] string{}
	for {
		//格式是 1(037)15286361196(037)xx(037)xx\r\n
		line, err := bfrd.ReadBytes('\n')
		if len(line)>2{
			line = line[:len(line)-2]//将\r\n，减去\r\n字符 所以要减-2
		} else{
			Logger(filePath + " 文件号码总数：" + strconv.Itoa(len(numsStr)))
			return users,numsStr, nil
		}
		var number string
		infos := bytes.Split(line,[]byte{037})

		if len(infos[1]) == 11 {
			number = string(infos[1])
			user:=&TPeUser{}
			user.Phone,err = strconv.ParseInt(number,10,64)
			if err!=nil{
				//要效率可以去掉日志
				Logger("不规范电话格式 error :" + err.Error()+","+string(line))
				continue
			}
			numsStr = append(numsStr, number)
			users = append(users, user)
		} else {
			number = string(line)
			Logger(number)
		}

		if err != nil { //遇到任何错误立即返回，并忽略 EOF 错误信息
			if err == io.EOF {
				Logger(filePath + " 文件号码总数：" + strconv.Itoa(len(numsStr)))
				return users,numsStr, nil
			}
			Logger("read file error:" + filePath)
			return nil,nil, err
		}
	}
	Logger(filePath + " 文件号码总数：" + strconv.Itoa(len(numsStr)))
	return users,numsStr, nil
}

//压缩文件
func CompressFile(formatFilePath string) error {
	//zipPath ="./formatFiles/"+zipPath
	cmd := exec.Command("gzip", formatFilePath)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		Logger("compressFile Error:can not obtain stdout pipe for command")
		return err
	}
	//执行命令
	if err := cmd.Start(); err != nil {
		Logger("compressFile Error:The command is err")
		return err
	}
	//读取所有输出
	_, err = ioutil.ReadAll(stdout)
	if err != nil {
		return err
	}
	if err := cmd.Wait(); err != nil {
		Logger("compressFile wait error")
		fmt.Println(err)
		return err
	}
	Logger("compressFile success:" + formatFilePath + ".gz")
	return nil
}

func SyncLoggerNum(strContent string) {
	go func(str string) {
		defer func() {
			recover()
		}()
		Logger(str)
	}(strContent)
}

//FTP-Put操作
//func FtpPutFile(config *Config, fileName string) error {
//	basePath := "./formatFiles/" + fileName
//	toPath := "./" + fileName
//	entry, err := ftp.Connect(config.ToFtpHost)
//	defer entry.Quit()
//	if err != nil {
//		Logger("connect to ftp server error :" + config.ToFtpHost)
//		return err
//	}
//	Logger("connect to ftp server success :" + config.ToFtpHost)
//	//login
//	entry.Login(config.ToFtpLoginUser, config.ToFtpLoginPassword)
//	if err != nil {
//		Logger("ftp login error, user:" + config.ToFtpLoginUser + ";pass: " + config.ToFtpLoginPassword)
//		fmt.Println(err)
//		return err
//	}
//	Logger("ftp login success")
//	file, err := ioutil.ReadFile(basePath)
//	buf := bytes.NewReader(file)
//	err = entry.Stor(toPath, buf)
//	if err != nil {
//		Logger("upload file to ftp server error :" + basePath)
//		return err
//	}
//	return nil
//}

//SFTP-PUT 操作
//func SFtpPutFile(config *Config, fileName string) error {
//	basePath := "./formatFiles/" + fileName
//	//
//	cmd := exec.Command("sh", "./sftpput.sh", config.ToFtpHost, config.ToFtpLoginUser, config.ToFtpLoginPassword, "./", basePath)
//	stdout, err := cmd.StdoutPipe()
//	if err != nil {
//		Logger("sftp stdout error")
//		return err
//	}
//	//执行命令
//	//err = cmd.Run()
//	//if err != nil {
//	//	Logger("Error:The command is err")
//	//	fmt.Println(err)
//	//	return err
//	//}
//	if err := cmd.Start(); err != nil {
//		Logger("Error:The command is err")
//		return err
//	}
//	//读取所有输出
//	str, err := ioutil.ReadAll(stdout)
//	if err != nil {
//		return err
//	}
//	mes := string(str)
//	fmt.Println(mes)
//	if err := cmd.Wait(); err != nil {
//		if strings.Contains(mes, "success") {
//			Logger("put sftp file success:")
//			fmt.Println(err)
//			return nil
//		}
//		Logger("wait error:" + string(str) + ";")
//		fmt.Println(err)
//		return err
//	}
//	Logger("put sftp file success:")
//	return nil
//}

//FTPs-Put 操作
//func FtpsPutFile(config *Config, fileName string) error {
//	basePath := "./formatFiles/" + fileName
//	//
//	cmd := exec.Command("sh", "./ftpsput.sh", config.FtpsCertFile, config.FtpsKeyFile, config.ToFtpHost, config.ToFtpLoginUser, config.ToFtpLoginPassword, basePath)
//	stdout, err := cmd.StdoutPipe()
//	if err != nil {
//		Logger("ftps stdout error")
//		return err
//	}
//	if err := cmd.Start(); err != nil {
//		Logger("Error:The command is err")
//		return err
//	}
//	//读取所有输出
//	str, err := ioutil.ReadAll(stdout)
//	if err != nil {
//		return err
//	}
//	mes := string(str)
//	fmt.Println(mes)
//	if err := cmd.Wait(); err != nil {
//		if strings.Contains(mes, "success") {
//			Logger("put ftps file success:")
//			fmt.Println(err)
//			return nil
//		}
//		Logger("wait error:" + string(str) + ";")
//		fmt.Println(err)
//		return err
//	}
//	Logger("put ftps file success:")
//	return nil
//}
