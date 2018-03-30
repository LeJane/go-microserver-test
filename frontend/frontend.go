//微服务前端
package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
)

const indexPage = "<html><head><title>Upload file</title></head><body><form enctype=\"multipart/form-data\" action=\"submitTask\" method=\"post\"> <input type=\"file\" name=\"uploadfile\" /> <input type=\"submit\" value=\"upload\" /> </form> </body> </html>"

var keyValueStoreAddress string
var masterAddress string

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Error: too few arguments.")
		return
	}
	keyValueStoreAddress = os.Args[1]
	resp, err := http.Get("http://" + keyValueStoreAddress + "/get?key=masterAddress")
	if err != nil || resp.StatusCode != http.StatusOK {
		fmt.Println("Error: can't get master address.", err)
		fmt.Println(resp.Body)
		return
	}

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
		return
	}

	masterAddress = string(data)
	if len(masterAddress) == 0 {
		fmt.Println("Error: can't get master address . Length is zero.")
		return
	}

	http.HandleFunc("/", handleIndex)
	http.HandleFunc("/submitTask", handleTask)
	http.HandleFunc("/isReady", handleCheckForReadiness)
	http.HandleFunc("/getImage", serveImage)
	http.ListenAndServe(":80", nil)
}

func handleIndex(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, indexPage)
}

func handleTask(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		err := r.ParseMultipartForm(10000000)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Println(err)
			fmt.Fprint(w, "Error: Wrong input")
			return
		}

		file, _, err := r.FormFile("uploadfile")
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Println(err)
			fmt.Fprint(w, "Error: Wrong input ")
			return
		}

		resp, err := http.Post("http://"+masterAddress+"/new", "image", file)
		if err != nil {
			fmt.Println("1", err)
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error: ", err)
			return
		}

		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			fmt.Println("2", err)
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error: ", err)
			return
		}

		fmt.Fprint(w, string(data))
	} else {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error: Not support method.")
	}
}

func handleCheckForReadiness(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		values, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error: ", err)
			return
		}

		if len(values.Get("id")) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error: Wrong input.")
			return
		}

		resp, err := http.Post("http://"+masterAddress+"/isReady?id="+values.Get("id")+"", "", nil)
		if err != nil || resp.StatusCode != http.StatusOK {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error: ", err)
			return
		}
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error: ", err)
			return
		}

		switch string(data) {
		case "0":
			fmt.Fprint(w, "Your image is not ready yet.")
		case "1":
			fmt.Fprint(w, "Your image is ready.")
		default:
			fmt.Fprint(w, "Internal server error")
		}

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
			fmt.Fprint(w, "Error: ", err)
			return
		}

		if len(values.Get("id")) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error: Wrong input.")
			return
		}

		resp, err := http.Post("http://"+masterAddress+"/get?id="+values.Get("id")+"&state=finished", "", nil)
		if err != nil || resp.StatusCode != http.StatusOK {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error: ", err)
			return
		}

		_, err = io.Copy(w, resp.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error: ", err)
			return
		}
	} else {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error: Not support method.")
	}
}
