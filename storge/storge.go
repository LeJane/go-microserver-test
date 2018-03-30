//存储微服务
package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
)

var dir string = "../tmp"

func main() {
	if !registerInKvStore() {
		return
	}

	http.HandleFunc("/sendImage", receiveImage)
	http.HandleFunc("/getImage", serveImage)
	http.ListenAndServe(":3002", nil)
}

func receiveImage(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		values, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error:", err)
			return
		}

		//如果是post的话，必须存在id和state参数，state只能为->working,finished
		if len(values.Get("id")) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error: Wrong input id.")
			return
		}

		if values.Get("state") != "working" && values.Get("state") != "finished" {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error:Wrong input state.")
			return
		}

		//检查id是否为int
		_, err = strconv.Atoi(values.Get("id"))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error: Wrong input id.")
			return
		}

		state := values.Get("state")
		DestDir := filepath.Join(dir, state)

		err = os.MkdirAll(DestDir, 0755)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error:", err)
			return
		}
		file, err := os.Create(DestDir + "/" + values.Get("id") + ".png")
		defer file.Close()
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error:", err)
			return
		}

		_, err = io.Copy(file, r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error:", err)
			return
		}

		fmt.Fprint(w, "success")

	} else {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error: Not support method.")
	}
}

func serveImage(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		values, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error:", err)
			return
		}

		if len(values.Get("id")) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Wrong input id.")
			return
		}

		if values.Get("state") != "working" && values.Get("state") != "finished" {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error: Wrong input state.")
			return
		}

		_, err = strconv.Atoi(values.Get("id"))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error: Wrong input id.")
			return
		}

		//打开文件
		state := values.Get("state")
		DestDir := filepath.Join(dir, state)

		file, err := os.Open(DestDir + "/" + values.Get("id") + ".png")
		defer file.Close()
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error:", err)
			return
		}

		_, err = io.Copy(w, file)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error:", err)
			return
		}

	} else {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error: Not support method.")
	}
}

//注册进入键值存储
func registerInKvStore() bool {
	//要将存储地址注册进入键值存储中，则需要存储地址，以及键值存储地址
	if len(os.Args) < 3 {
		fmt.Println("Error:Too few arguments.")
		return false
	}

	storeAddress := os.Args[1]
	keyValueStoreAddress := os.Args[2]

	resp, err := http.Post("http://"+keyValueStoreAddress+"/set?key=storeAddress&value="+storeAddress+"", "", nil)
	if err != nil {
		fmt.Println("Error:", err)
		return false
	}

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Error:", err)
		return false
	}

	if resp.StatusCode != http.StatusOK {
		fmt.Println("Error:failure when contacting key-value store: ", string(data))
		return false
	}

	return true
}
