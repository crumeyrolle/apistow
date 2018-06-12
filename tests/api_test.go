package tests

import (
	"fmt"
	"log"
	"os"
	"reflect"
	"time"

	"github.com/pcrume/apistow"
	"github.com/stretchr/testify/assert"

	"testing"
)

func Test_Connect(t *testing.T) {
	client := new(apistow.Location)
	s1 := "OVH"
	s2 := "OVH"
	err := client.Connect(s1, s2)
	assert.Nil(t, err)

}

// cd ~/go/src/github.com/pcrume/tests
//go test -v
func Test_Workflow(t *testing.T) {
	var metadata = make(map[string]interface{})
	var fileNameTest = "/tmp/test.txt"
	var fileNameTestExtract = "/tmp/testExtracted.txt"
	timed := time.Now()
	formatedTime := timed.Format(time.RFC1123)
	metadata["time"] = formatedTime
	metadata["company"] = "CS"
	metadata["user"] = "itisfortested"
	metadata["country"] = "France"
	metadata["Split"] = ""
	client := new(apistow.Location)
	s1 := "OVH"
	s2 := "OVH"
	err := client.Connect(s1, s2)
	if err != nil {
		t.Fatal()
	}
	assert.Nil(t, err)
	lContainersItemsDeb, err := client.Inspect()
	myContainerTest := "ContainertestONE"
	myItemTest1 := "ItemtestONE"
	myItemTest2 := "ItemtestTWO"
	myItemTest3 := "ItemtestTHREE"
	myItemTestSplit := "ItemtestSplitted"
	err = client.Create(myContainerTest)
	assert.Nil(t, err)
	f, sizefile, err := createAndReadforTest(fileNameTest)
	assert.Nil(t, err)
	err = client.PutItem(myContainerTest, myItemTest1, f, metadata)
	assert.Nil(t, err)
	size, err := client.ItemSize(myContainerTest, myItemTest1)
	assert.Equal(t, size, sizefile)
	assert.Nil(t, err)
	f, sizefile, err = createAndReadforTest(fileNameTest)
	assert.Nil(t, err)
	err = client.PutItem(myContainerTest, myItemTest2, f, metadata)
	assert.Nil(t, err)
	size, err = client.ItemSize(myContainerTest, myItemTest2)
	assert.Equal(t, size, sizefile)
	fw, err := create(fileNameTestExtract)
	err = client.ExtractItem(myContainerTest, myItemTest2, fw, nil, nil)
	_, sizefile, err = read(fileNameTestExtract)
	assert.Equal(t, size, sizefile)
	assert.Nil(t, err)
	f, sizefile, err = createAndReadforTest(fileNameTest)
	assert.Nil(t, err)
	err = client.PutItem(myContainerTest, myItemTest3, f, metadata)
	assert.Nil(t, err)
	/***/
	pattern := "*isfortes*"
	lContainersItemsFiltered, err := client.FilterByMetadata("user", pattern)
	affichRes(client, lContainersItemsFiltered)
	assert.Nil(t, err)
	/****/
	item, err := client.GetItem(myContainerTest, myItemTest3)
	assert.Nil(t, err)
	meta, err := client.ItemMetadata(myContainerTest, item.Name())
	f, sizefile, err = read(fileNameTest)
	assert.Nil(t, err)
	err = client.PutItemByChunk(myContainerTest, myItemTestSplit, 10, f, meta)
	assert.Nil(t, err)
	time.Sleep(20 * time.Second)
	err = client.Clear(myContainerTest)
	assert.Nil(t, err)
	time.Sleep(20 * time.Second)
	pattern = "*isfortes*"
	lContainersItemsFiltered, err = client.FilterByMetadata("user", pattern)
	assert.Nil(t, err)
	assert.Empty(t, lContainersItemsFiltered)
	err = client.Remove(myContainerTest)
	assert.Nil(t, err)
	lContainersItemsFin, err := client.Inspect()
	assert.Nil(t, err)
	//affichRes(client, lContainersItemsFin)
	eq := reflect.DeepEqual(lContainersItemsDeb, lContainersItemsFin)
	assert.True(t, eq)

}

func affichRes(client *apistow.Location, msoftabs map[string][]string) {
	for k, v := range msoftabs {
		fmt.Printf("Conteneur : %s \n", k)
		for l := range v {
			sizeIt, err := client.ItemSize(k, v[l])
			meta, err := client.ItemMetadata(k, v[l])
			if err != nil {
				fmt.Println(err)
			}
			fmt.Printf(" Items  %s Size : %d  Metadata : %s \n", v[l], sizeIt, meta)
		}
	}
}

func createAndReadforTest(fileName string) (f *os.File, size int64, err error) {
	f, err = os.Create(fileName)
	if err != nil {
		log.Fatalf("failed creating file: %s", err)
		return f, size, err
	}
	_, err = f.WriteString("Content string for testing item file creation")
	if err != nil {
		log.Fatalf("failed writing to file: %s", err)
		return f, size, err
	}
	f.Sync()
	f, size, err = read(fileName)
	if err != nil {
		log.Fatalf("read  file: %s", err)
		return f, size, err
	}
	return f, size, err
}

func create(fileName string) (f *os.File, err error) {
	f, err = os.Create(fileName)
	if err != nil {
		fmt.Println(f, err)
		return f, err
	}
	//defer f.Close()
	return f, err
}

func read(fileName string) (f *os.File, size int64, err error) {
	f, err = os.Open(fileName)
	if err != nil {
		fmt.Println(f, err)
		return f, size, err
	}
	fileName = f.Name()
	fi, err := os.Stat(fileName)
	if err != nil {
		return f, size, err
	}
	// get the size
	size = fi.Size()
	return f, size, err
}
