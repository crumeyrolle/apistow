package apistow

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/graymeta/stow"
	_ "github.com/graymeta/stow/s3"
	_ "github.com/graymeta/stow/swift"
)

// split body and spec to do
//  testify assert to do
// gestion tenant file dans connect to do

//ObjectStorageAPI ObjectStorageAPI
type ObjectStorageAPI interface {
	Connect(projectName string, provider string) error

	Inspect() (map[string][]string, error)
	FilterByMetadata(key string, valuePattern string) (map[string][]string, error)

	ListContainers() ([]string, error)

	ListItems(ContainerName string) (map[string][]string, error)
	FilterItemsByMetadata(ContainerName string, key string, pattern string) (map[string][]string, error)

	Create(ContainerName string) error
	Remove(ContainerName string) error
	Clear(myContainerName string) error

	PutItemByChunk(container string, itemName string, chunksize int, f *os.File, metadata map[string]interface{}) error
	PutItem(container string, itemName string, f *os.File, metadata map[string]interface{}) error
	PutItemContent(container string, itemName string, content []byte, metadata map[string]interface{}) error

	ExtractItem(container string, itemName string, f *os.File, pseekTo *int64, plength *int64) error
	ExtractItemContent(container string, itemName string) ([]byte, error)

	ItemSize(ContainerName string, item string) (int64, error)
	ItemEtag(ContainerName string, item string) (string, error)
	ItemLastMod(ContainerName string, item string) (time.Time, error)
	ItemID(ContainerName string, item string) (id string)
	ItemMetadata(ContainerName string, item string) (map[string]interface{}, error)
}

//Location Location
type Location struct {
	Location StowLocation
}

//StowLocation StowLocation
type StowLocation struct {
	Location stow.Location
}

//Connect Connect
func (client *Location) Connect(projectName string, provider string) (err error) {

	var kind string
	var config stow.ConfigMap
	// on linux before ~/go/src/github.com/graymeta/stow/main$ go run main.go
	//export PROVIDER=orange
	//export PROVIDER=Flexibleengine
	//export PROVIDER=ovh

	//provider := os.Getenv("PROVIDER")

	if provider == "Flexibleengine" {
		kind = "s3"
		domain := "OCB0001829"
		tenantAuthURL := "oss.eu-west-0.prod-cloud-ocb.orange-business.com"
		tenantName := "05d285cfbc4b439eb06af12611adb2a8"
		region := "eu-west-0"
		accessKeyID := "RUMOGGWCM0KCMDX6K9GW"
		secretKey := "0r9lp1fFQ8WyUSyb7Z7xOlOa1jHca7f1cSppiXaP"
		endpoint := "oss.eu-west-0.prod-cloud-ocb.orange-business.com"

		config = stow.ConfigMap{
			"access_key_id":   accessKeyID,
			"secret_key":      secretKey,
			"region":          region,
			"tenant_name":     tenantName,
			"tenant_auth_url": tenantAuthURL,
			"domain":          domain,
			"endpoint":        endpoint,
		}

	}
	if provider == "OVH" {
		kind = "swift"
		username := "Yfk9h7THvsuD"
		key := "AnrVjJJuyFxVUNXG95f9FbYkw6xSuCRJ"
		tenantName := "3670559383264836"
		tenantAuthURL := "https://auth.cloud.ovh.net/v3/"
		region := "SBG3"
		domain := "default"

		config = stow.ConfigMap{
			"username":        username,
			"key":             key,
			"tenant_name":     tenantName,
			"tenant_auth_url": tenantAuthURL,
			"region":          region,
			"domain":          domain,
			"kind":            kind,
		}
	}
	if provider == "CLOUDWat" {
		kind = "swift"
		username := "eric.guzzonato@c-s.fr"
		key := "{4f8uMwAe.j]>aRg"
		tenantName := "0750190889_COPERNICUS-1"
		tenantAuthURL := "https://identity.fr1.cloudwatt.com/v2.0"
		region := "fr1"
		domain := "default"

		config = stow.ConfigMap{
			"username":        username,
			"key":             key,
			"tenant_name":     tenantName,
			"tenant_auth_url": tenantAuthURL,
			"region":          region,
			"domain":          domain,
			"kind":            kind,
		}

	}
	// Check config location
	err = stow.Validate(kind, config)
	if err != nil {
		fmt.Println("erreur Validate", err)
	}
	client.Location.Location, err = stow.Dial(kind, config)
	if err != nil {
		fmt.Println("erreur Dial", err, client.Location.Location)
	}

	tenantAuthURL, ok := config.Config("tenant_auth_url")
	if ok != true {
		log.Println("Container WalkContainers => tenantAuthUrl undefined")
	}

	//MODIF PC cas Flexibleengine si tenantAuthUrl contient "prod-cloud-ocb.orange-business.com"
	if strings.Contains(tenantAuthURL, "prod-cloud-ocb.orange-business.com") == true {
		provider = "Flexibleengine"
	}
	if tenantAuthURL == "https://auth.cloud.ovh.net/v3/" {
		provider = "OVH"
	}
	if tenantAuthURL == "https://identity.fr1.cloudwatt.com/v2.0" {
		provider = "CLOUD Wat"
	}
	return err
}

// ItemSize ItemSize
func (client *Location) ItemSize(ContainerName string, item string) (sizeIt int64, err error) {
	itemstow, err := client.GetItem(ContainerName, item)
	if err != nil {
		fmt.Println("erreur GetItem : ", ContainerName, '.', item, err)
		return sizeIt, err
	}
	sizeIt, err = stow.Item.Size(itemstow)
	if err != nil {
		fmt.Println("erreur size item : ", ContainerName, '.', item, err)
		return sizeIt, err
	}
	return sizeIt, err
}

// ItemEtag ItemEtag
func (client *Location) ItemEtag(ContainerName string, item string) (ETag string, err error) {
	itemstow, err := client.GetItem(ContainerName, item)
	if err != nil {
		fmt.Println("erreur GetItem : ", ContainerName, '.', item, err)
		return ETag, err
	}
	ETag, err = stow.Item.ETag(itemstow)
	if err != nil {
		fmt.Println("erreur Etag item : ", ContainerName, '.', item, err)
		return ETag, err
	}
	return ETag, err
}

// ItemLastMod ItemLastMod
func (client *Location) ItemLastMod(ContainerName string, item string) (tim time.Time, err error) {
	itemstow, err := client.GetItem(ContainerName, item)
	if err != nil {
		fmt.Println("erreur GetItem : ", ContainerName, '.', item, err)
		return tim, err
	}
	tim, err = stow.Item.LastMod(itemstow)
	if err != nil {
		fmt.Println("erreur LastModTime item : ", ContainerName, '.', item, err)
		return tim, err
	}
	return tim, err
}

// ItemID ItemID
func (client *Location) ItemID(ContainerName string, item string) (id string, err error) {
	itemstow, err := client.GetItem(ContainerName, item)
	if err != nil {
		fmt.Println("erreur GetItem : ", ContainerName, '.', item, err)
		return id, err
	}
	id = stow.Item.ID(itemstow)
	return id, err
}

// ItemMetadata ItemMetadata
func (client *Location) ItemMetadata(ContainerName string, item string) (meta map[string]interface{}, err error) {
	itemstow, err := client.GetItem(ContainerName, item)
	if err != nil {
		fmt.Println("erreur GetItem : ", ContainerName, '.', item, err)
		return meta, err
	}
	meta, err = stow.Item.Metadata(itemstow)
	if err != nil {
		fmt.Println("erreur MetadataItem item : ", ContainerName, '.', item, err)
		return meta, err
	}
	return meta, err
}

//GetItem GetItem
func (client *Location) GetItem(ContainerName string, item string) (myItem stow.Item, err error) {
	c1, err := client.Location.Location.Container(ContainerName)
	if err != nil {
		fmt.Println("erreur location.Container : ", ContainerName, err)
		return myItem, err
	}
	myItem, err = stow.Container.Item(c1, item)
	return myItem, err
}

//Remove Remove
func (client *Location) Remove(ContainerName string) (err error) {
	err = client.Location.Location.RemoveContainer(ContainerName)
	if err != nil {
		fmt.Println("erreur RemoveContainer : ", ContainerName, err)
		return err
	}
	return err
}

//Create Create
func (client *Location) Create(ContainerName string) (err error) {
	_, err = client.Location.Location.CreateContainer(ContainerName)
	if err != nil {
		fmt.Println("erreur CreateContainer : ", ContainerName, err)
		return err
	}
	return err
}

//SearchPatternForMapUser SearchPatternForMapUser
func SearchPatternForMapUser(key string, pattern string, m map[string]interface{}) bool {
	find := false
	for k := range m {
		str := fmt.Sprintf("%v", m[k])
		matched, err := filepath.Match(pattern, str)
		if matched == true && key == k {
			find = true
		}
		if err != nil {
			fmt.Println("err", err)
			find = false
		}
	}
	return find
}

// Inspect Liste List All Containers And Items
func (client *Location) Inspect() (s map[string][]string, err error) {
	var oneItemFund = false
	//vsf := make([]string, 0)
	vsf := make(map[string][]string)
	//fmt.Println("WalkContainers")
	err = stow.WalkContainers(client.Location.Location, stow.NoPrefix, 100,
		func(c stow.Container, err error) error {
			if err != nil {
				return err
			}
			//log.Println("Nom du Container  => : ", c.Name())
			/***/
			err = stow.Walk(c, stow.NoPrefix, 100,
				func(item stow.Item, err error) error {
					if err != nil {
						return err
					}
					//log.Println("   Item => : ", item.Name(), " Metadata Item => ", meta)
					oneItemFund = true
					//vsf = append(vsf, c.Name())
					//vsf = append(vsf, item.Name())
					vsf[c.Name()] = append(vsf[c.Name()], item.Name())

					return nil
				})
			if oneItemFund == false {
				//log.Println(" No Item found corresponding to filter", key, pattern)
			}

			return nil
		})
	if err != nil {
		log.Println("Container WalkContainers => : ", err)
	}
	return vsf, err
}

// FilterByMetadata Liste List All Containers And Items byPattern
func (client *Location) FilterByMetadata(key string, pattern string) (s map[string][]string, err error) {
	var oneItemFund = false
	//vsf := make([]string, 0)
	vsf := make(map[string][]string)
	//fmt.Println("WalkContainers")
	err = stow.WalkContainers(client.Location.Location, stow.NoPrefix, 100,
		func(c stow.Container, err error) error {
			if err != nil {
				return err
			}

			err = stow.Walk(c, stow.NoPrefix, 100,
				func(item stow.Item, err error) error {
					if err != nil {
						return err
					}
					meta := make(map[string]interface{})
					meta, err = stow.Item.Metadata(item)
					trouve := SearchPatternForMapUser("user", pattern, meta)
					if trouve == true {
						//log.Println("   Item => : ", item.Name(), " Metadata Item => ", meta)
						oneItemFund = true
						//vsf = append(vsf, c.Name())
						//vsf = append(vsf, item.Name())
						vsf[c.Name()] = append(vsf[c.Name()], item.Name())
					}
					return nil
				})
			if oneItemFund == false {
			}

			return nil
		})
	if err != nil {
		log.Println("Container WalkContainers => : ", err)
	}
	return vsf, err
}

// ListContainers ListContainers
func (client *Location) ListContainers() (s []string, err error) {
	vsf := make([]string, 0)
	err = stow.WalkContainers(client.Location.Location, stow.NoPrefix, 100,
		func(c stow.Container, err error) error {
			if err != nil {
				return err
			}

			vsf = append(vsf, c.Name())
			return nil
		})
	if err != nil {
		log.Println("Container WalkContainers => : ", err)
	}
	return vsf, err
}

// FilterItemsByMetadata  FilterItemsByMetadata
func (client *Location) FilterItemsByMetadata(ContainerName string, key string, pattern string) (s map[string][]string, err error) {
	var oneItemFund = false
	//vsf := make([]string, 0)
	vsf := make(map[string][]string)
	c, err := client.Location.Location.Container(ContainerName)
	if err != nil {
		log.Println(" Location.Container => : ", ContainerName, err)
		return vsf, err
	}

	err = stow.Walk(c, stow.NoPrefix, 100,
		func(item stow.Item, err error) error {
			if err != nil {
				return err
			}
			meta := make(map[string]interface{})
			meta, err = stow.Item.Metadata(item)
			trouve := SearchPatternForMapUser("user", pattern, meta)
			if trouve == true {
				//log.Println("   Item => : ", item.Name(), " Metadata Item => ", meta)
				oneItemFund = true
				//vsf = append(vsf, c.Name())
				//vsf = append(vsf, item.Name())
				vsf[c.Name()] = append(vsf[c.Name()], item.Name())
			}
			return nil
		})
	if oneItemFund == false {
		log.Println(" No Item found corresponding to filter", key, pattern)
	}
	return vsf, err
}

// ListItems  ListItems
func (client *Location) ListItems(ContainerName string) (s map[string][]string, err error) {
	//vsf := make([]string, 0)
	vsf := make(map[string][]string)
	c, err := client.Location.Location.Container(ContainerName)
	if err != nil {
		log.Println(" Location.Container => : ", ContainerName, err)
		return vsf, err
	}
	//log.Println(" Location.Container => : ", c.Name())
	err = stow.Walk(c, stow.NoPrefix, 100,
		func(item stow.Item, err error) error {
			if err != nil {
				return err
			}
			//log.Println("   Item => : ", item.Name())
			//vsf = append(vsf, c.Name())
			//vsf = append(vsf, item.Name())
			vsf[c.Name()] = append(vsf[c.Name()], item.Name())
			return nil
		})
	return vsf, err
}

// Clear  Clear
func (client *Location) Clear(myContainerName string) (err error) {
	c1, err := client.Location.Location.Container(myContainerName)
	if err != nil {
		log.Println(" Location.Container => : ", myContainerName, err)
		return err
	}
	log.Println(" Location.Container => : ", c1.Name())
	err = stow.Walk(c1, stow.NoPrefix, 100,
		func(item stow.Item, err error) error {
			if err != nil {
				return err
			}
			log.Println(" delete Item => : ", item.Name(), " for Container ", c1.Name())
			err = stow.Container.RemoveItem(c1, item.Name())
			return nil
		})
	return err
}

// ExtractItem ExtractItem
func (client *Location) ExtractItem(container string, itemName string, f *os.File, pseekTo *int64, plength *int64) (err error) {

	var seekTo int64
	var length int64
	defer f.Close()
	c1, err1 := client.Location.Location.Container(container)
	if err1 != nil {
		fmt.Println("erreur location.Container : ", container, err1)
		return err1
	}
	myItem, err := stow.Container.Item(c1, itemName)
	if err != nil {
		return err
	}
	sizeIt, err := stow.Item.Size(myItem)
	if err != nil {
		return err
	}

	if pseekTo == nil {
		seekTo = 0
	} else {
		seekTo = *pseekTo
	}

	if plength == nil {
		length = sizeIt
	} else {
		length = *plength
	}
	log.Printf("ExtractItem   %s.%s extracted until %d bytes to %d bytes to %s ", container, itemName, seekTo, length, f.Name())
	err = stow.Walk(c1, stow.NoPrefix, 100,
		func(item stow.Item, err error) error {
			if err != nil {
				return err
			}
			//fmt.Println(item.Name())
			if item.Name() == itemName {
				r, err := item.Open()
				if err != nil {
					fmt.Println(r, err)
					return err
				}
				defer r.Close()

				if seekTo == 0 && length >= sizeIt {
					nbytes, err := io.CopyN(f, r, sizeIt)
					if err != nil {
						fmt.Println(r, err)
						return err
					}
					f.Sync()
					log.Println(" Extract Item By BytesRange => ", container, item.Name(), " wrote ", nbytes, " to ", f.Name())
				} else {

					buf := make([]byte, seekTo)

					if _, err := io.ReadAtLeast(r, buf, int(seekTo)); err != nil {
						log.Fatal(err)
					}

					bufbis := make([]byte, length)
					if _, err := io.ReadAtLeast(r, bufbis, int(length)); err != nil {
						fmt.Println("error ")
						log.Fatal(err)
					}

					rbis := bytes.NewReader(bufbis)
					nbytes, err := io.CopyBuffer(f, rbis, bufbis)
					if err != nil {
						fmt.Println(r, err)
						return err
					}
					f.Sync()
					log.Println(" Extract Item By BytesRange => ", container, item.Name(), " wrote ", nbytes, " to ", f.Name())
				}
			}
			return nil
		})
	return err
}

// ExtractItemContent ExtractItemContent
func (client *Location) ExtractItemContent(container string, itemName string) (content []byte, err error) {

	c1, err1 := client.Location.Location.Container(container)
	if err1 != nil {
		fmt.Println("erreur location.Container : ", container, err1)
		return content, err1
	}
	myItem, err := stow.Container.Item(c1, itemName)
	if err != nil {
		return content, err
	}

	err = stow.Walk(c1, stow.NoPrefix, 100,
		func(item stow.Item, err error) error {
			if err != nil {
				return err
			}
			//fmt.Println(item.Name())
			if item.Name() == itemName {
				r, err := item.Open()
				if err != nil {
					fmt.Println(r, err)
					return err
				}
				defer r.Close()
				sizeIt, err := stow.Item.Size(myItem)
				if err != nil {
					fmt.Println("erreur size item : ", container, '.', itemName, err)
					return err
				}

				content = make([]byte, sizeIt)
				io.ReadFull(r, content)
				log.Println(" ExtractItemContent => ", container, ".", myItem.Name(), " wrote to ", string(content[:]))
			}
			return nil
		})
	if err != nil {
		log.Println("ExtractItemContent => : ", err)
	}
	return content, err
}

// PutItem PutItem
func (client *Location) PutItem(container string, itemName string, f *os.File, metadata map[string]interface{}) (err error) {

	fmt.Println("PutItem => ", container, ".", itemName, " from ", f.Name())
	c1, err1 := client.Location.Location.Container(container)
	if err1 != nil {
		fmt.Println("erreur location.Container : ", container, err1)
		return err1
	}

	fileName := f.Name()
	fi, e := os.Stat(fileName)
	if e != nil {
		return e
	}
	// get the size
	size := fi.Size()
	defer f.Close()

	//b, err := ioutil.ReadAll(uploadFile)
	//fmt.Println(b)
	if err != nil {
		log.Println("erreur read file on PutItem  ", container, ".", itemName, " from ", fileName)
	}
	r := bufio.NewReader(f)
	item, err := stow.Container.Put(c1, itemName, r, size, metadata)
	//fmt.Println(item.Name())
	if err != nil {
		log.Println("erreur stow.Container.Put ", item.Name(), err)
		return nil
	}

	return err
}

// PutItemContent PutItemContent
func (client *Location) PutItemContent(container string, itemName string, content []byte, metadata map[string]interface{}) (err error) {

	fmt.Println("PutItemContent => ", container, ".", itemName, " from ", string(content[:]))
	c1, err1 := client.Location.Location.Container(container)
	if err1 != nil {
		fmt.Println("erreur location.Container : ", container, err1)
		return err1
	}
	r := bytes.NewReader(content)
	size := int64(len(content))
	_, err = stow.Container.Put(c1, itemName, r, size, metadata)
	if err != nil {
		log.Println("erreur stow.Container.Put ", itemName, err)
		return nil
	}

	return err
}

func split(buf []byte, lim int) [][]byte {
	var chunk []byte
	chunks := make([][]byte, 0, len(buf)/lim+1)
	for len(buf) >= lim {
		chunk, buf = buf[:lim], buf[lim:]
		chunks = append(chunks, chunk)
	}
	if len(buf) > 0 {
		chunks = append(chunks, buf[:len(buf)])
	}
	return chunks
}

//BytesBufferToItem BytesBufferToItem
func BytesBufferToItem(i int, location stow.Location, container string, bufferedReader io.Reader, byteSlice []byte, itemName string, size int, numBytesRead int, metadata map[string]interface{}) (err error) {
	p := make([]byte, numBytesRead)
	c1, err1 := location.Container(container)
	if err1 != nil {
		fmt.Println("erreur location.Container : ", container, err1)
		return err1
	}
	n, err := bufferedReader.Read(p)
	if err == io.EOF {
		return err
	}
	r := bytes.NewReader(p)
	//fmt.Println(" buffer ", string(p[:n]))
	itemNamePart := itemName + strconv.Itoa(i)
	metadata["Split"] = itemName
	item, err := stow.Container.Put(c1, itemNamePart, r, int64(n), metadata)
	if err != nil {
		log.Printf("File : %s split %d bytes:  erreur %s \n", item.Name(), numBytesRead, err)
		return err
	}
	log.Printf("File : %s split %d \n", item.Name(), numBytesRead)
	return err
}

// PutItemByChunk PutItemByChunk
func (client *Location) PutItemByChunk(container string, itemName string, chunkSize int, f *os.File, metadata map[string]interface{}) (err error) {
	log.Printf("Generate multi part  %s.%s* from %s spliting by %d bytes parts", container, itemName, f.Name(), chunkSize)
	c1, err1 := client.Location.Location.Container(container)
	if err1 != nil {
		fmt.Println("erreur location.Container : ", container, err1)
		return err1
	}

	fileName := f.Name()
	fi, e := os.Stat(fileName)
	if e != nil {
		fmt.Println(e)
		return e
	}
	// get the size
	size := fi.Size()
	defer f.Close()
	if err != nil {
		// reading file failed, handle appropriately
		log.Println("erreur read file on PutItem  ", container, ".", itemName, " from ", fileName)
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	bufferedReader := bufio.NewReader(f)
	p := make([]byte, chunkSize)
	var i int
	restbBytes := int(size)
	for {
		if restbBytes < chunkSize {
			chunkSize = restbBytes
		}
		err = BytesBufferToItem(i, client.Location.Location, c1.Name(), bufferedReader, p, itemName, int(size), chunkSize, metadata)
		restbBytes = restbBytes - chunkSize
		if restbBytes == 0 {
			break
		}
		i++
	}
	return err
}

func writeBuffToFile(byteSlice []byte, fileName string) (err error) {
	f, err := os.Create(fileName)
	if err != nil {
		fmt.Println(" erreur !!!!!! ", f, err)
		return err
	}
	bufferedWriter := bufio.NewWriter(f)
	bytesWritten, err := bufferedWriter.Write(byteSlice)
	if err != nil {
		log.Printf("erreur %s when Bytes written: %d for filename  %s \n", err, bytesWritten, fileName)
	}
	bufferedWriter.Flush()
	f.Close()
	return err
}
