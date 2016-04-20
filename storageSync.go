package main

import (
    "encoding/json"
	"io/ioutil"
	"log"
    // "fmt"
    "os"
    "strings"
    "errors"
    "time"

	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"

	"google.golang.org/cloud"
	"google.golang.org/cloud/storage"
	"google.golang.org/cloud/pubsub"
    
    "github.com/secsy/goftp"
)

// Configuration ...
type Configuration struct {
    GsProjectID     string `json:"gs_project_id"`
    GsBucket        string `json:"gs_bucket"`
    FtpAddr         string `json:"ftp_addr"`
    FtpUser         string `json:"ftp_user"`
    FtpPassword     string `json:"ftp_passwd"`
    FtpRoot         string `json:"ftp_root"`
    AllowStores     []string `json:"allow_stores"`
    CredentialsPath string
}

// SyncInfo ...
type SyncInfo struct {
    storeID     string
    orderID     string
    gcPrefix    string
    ftpRootPath string
}

// Gcloud ...
type Gcloud struct {
    ctx           context.Context
    storageClient storage.Client
}

// MaxFtpConnect ...
const MaxFtpConnect = 2

var config = Configuration{}
var gcloud = Gcloud{}
var currentFtpConnect = 0

func init() {
    configFilePath := os.Getenv("GSS_CONFIG")
    if (len(configFilePath) <= 0) {
        configFilePath = "config.json"
    }
    configFile, err := os.Open(configFilePath)
    if (err != nil) {
        log.Fatalln("Opening file failed. ", err)
    }
    decoder := json.NewDecoder(configFile)
    err = decoder.Decode(&config)
    if err != nil {
        log.Fatalln("Loading config failed.", err)
    }
    
    credentialsPath := os.Getenv("GSS_CREDENTIALS")
    if len(credentialsPath) <= 0 {
        log.Fatalln(`No set environment variable "GSS_CREDENTIALS" for credentials path.`)
    }
    
    config.CredentialsPath = credentialsPath
    
    gcloudAuth()
    
    // fmt.Println("GsBucket: ", config.GsBucket)
    // fmt.Println("FtpAddr: ", config.FtpAddr)
    // fmt.Println("FtpUser: ", config.FtpUser)
    // fmt.Println("FtpPassword: ", config.FtpPassword)
    // fmt.Println("FtpRoot: ", config.FtpRoot)
    // fmt.Println("AllowStores: ", config.AllowStores)
    // fmt.Println("CredentialsPath: ", config.CredentialsPath)
    log.Println("GSS initial success.")
}

func main() {
   
    for {
        msgs, err := pubsub.Pull(gcloud.ctx, "syncToFtp", 1)
        if err != nil {
            log.Println(err)
        }
        if len(msgs) > 0 {
            attr := msgs[0].Attributes
            storeID := attr["storeID"]
            orderID := attr["orderID"]
            
            if err := pubsub.Ack(gcloud.ctx, "syncToFtp", msgs[0].AckID); err != nil {
                log.Println("Ack failed.")
            }
            
            info, err := initSyncInfo(storeID, orderID)
            
            if err == nil {
                // fmt.Println("storeID: ", info.storeID)
                // fmt.Println("orderID: ", info.orderID)
                // fmt.Println("gcPrefix: ", info.gcPrefix)
                // fmt.Println("ftpRootPath: ", info.ftpRootPath)
                objects := queryStorageObjects(info)
                
                uploadObjects(info, objects)
            }
        }
    }
}

func uploadObjects(info *SyncInfo, objects *[]string) {
    ftpConfig := goftp.Config {
        User:               config.FtpUser,
        Password:           config.FtpPassword,
        ConnectionsPerHost: 10,
    }
    ftpClient, err := goftp.DialConfig(ftpConfig, config.FtpAddr)
    
    if err != nil {
        log.Fatalln("ftp connect failed. ", err)
    }
    
    //create dir
    makeDir(info, ftpClient, objects)
    
    doUpload := make(chan bool)
    uploadDone := make(chan bool)
    go func() {
        for {
            _, ok := <-uploadDone
            if ok {
                currentFtpConnect--
            } else {
                return
            }
        }
	}()
    
    // create upload job
    for _, obj := range *objects {
        go doUploadObject(info, obj, ftpClient, doUpload, uploadDone)
    }
    
    // send doUpload access
    for i := 0; i < len(*objects); i++ {
		for currentFtpConnect >= MaxFtpConnect {
            time.Sleep(time.Millisecond * 500)
        }
        doUpload <- true
        currentFtpConnect++
	}
    
    for currentFtpConnect > 0 {
        log.Println("currentFtpConnect: ", currentFtpConnect)
        time.Sleep(time.Millisecond * 500)
    }
}

func doUploadObject(info *SyncInfo, object string, ftpClient *goftp.Client, doUpload chan bool, uploadDone chan bool) {
    for {
        _,ok := <- doUpload
        if ok {
            ftpPath := getFtpPath(info, object)
            ftpPathStr := strings.Join(*ftpPath, "/")
            rc, err := gcloud.storageClient.Bucket(config.GsBucket).Object(object).NewReader(gcloud.ctx)
            if err != nil {
                log.Println("Get Object Read io failed.", err)
            } else {
                defer rc.Close()
                err := ftpClient.Store(ftpPathStr, rc)             
                if err != nil {
                    log.Println("Object store to FTP failed ", err)
                } else {
                    log.Println(ftpPathStr + " stror to FTP Success.")
                }
            }
        }
        uploadDone <- true
        return
    }
}

func initSyncInfo(storeID , orderID string) (*SyncInfo, error) {
    for _, allowStore := range config.AllowStores {
        if allowStore == storeID {
            gcPrefix := storeID + "/" + "order/" + orderID
            ftpRootPath := config.FtpRoot + "/" + orderID + "/"
            
            return &SyncInfo{
                storeID: storeID,
                orderID: orderID,
                gcPrefix: gcPrefix,
                ftpRootPath: ftpRootPath,
            }, nil
        }
    }
    
    return nil, errors.New("Not Allow Storage ID.")
}

func makeDir(info *SyncInfo, ftpClinet *goftp.Client, objects *[]string)  {
    for _, obj := range *objects {
        path := getFtpPath(info, obj)
        var mkPath string
        for i, dir := range *path {
	        mkPath += dir + "/"
            if i <= len(*path) - 2 {
	            ftpClinet.Mkdir(mkPath)
            }
        }
    }
}

func getFtpPath(info *SyncInfo, stroagePath string) *[]string {
    ftpPathStr := strings.Replace(stroagePath, info.gcPrefix + "/", "", -1)
    ftpPath := strings.Split(info.ftpRootPath + ftpPathStr, "/")
    return &ftpPath
}

func queryStorageObjects(info *SyncInfo) *[]string {
    var objects []string

    query := storage.Query{Prefix: info.gcPrefix}
    
    objList, err := gcloud.storageClient.Bucket(config.GsBucket).List(gcloud.ctx, &query)
    
    if (err != nil) {
        log.Println("List targe folder failed. " ,err)
        return &objects
    }
    for _, obj := range objList.Results {
        if !strings.HasSuffix(obj.Name, "/") {
            objects = append(objects, obj.Name)
        }
    }
    return &objects
}

func gcloudAuth() {
	jsonKey, err := ioutil.ReadFile(config.CredentialsPath)
	if err != nil {
		log.Fatal(err)
	}
	conf, err := google.JWTConfigFromJSON(
		jsonKey,
		pubsub.ScopeCloudPlatform,
		pubsub.ScopePubSub,
        storage.ScopeFullControl,
	)
	if err != nil {
		log.Fatal(err)
	}
    
	ctx := cloud.NewContext(config.GsProjectID, conf.Client(oauth2.NoContext))
	client, err := storage.NewClient(ctx, cloud.WithTokenSource(conf.TokenSource(ctx)))
    if err != nil {
        log.Fatal("New Storage client.", err)
    }
    
    gcloud.ctx = ctx
    gcloud.storageClient = *client
}