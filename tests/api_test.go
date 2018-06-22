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

// cd ~/go/src/github.com/pcrume/tests
//go test -v

func Test_Workflow(t *testing.T) {

	Flexeengine := new(apistow.Location)
	ovh := new(apistow.Location)
	cloudWatt := new(apistow.Location)

	err := Flexeengine.Connect("Flexibleengine", "Flexibleengine")
	err = ovh.Connect("OVH", "OVH")
	err = cloudWatt.Connect("CLOUDWat", "CLOUDWat")
	assert.Nil(t, err)

	count, err := Flexeengine.Count("", "*")
	fmt.Println("Number of Item : ", count, "Total size : ", Flexeengine.SumSize())
	count, err = ovh.Count("", "*")
	fmt.Println("Number of Item : ", count, "Total size : ", ovh.SumSize())
	count, err = cloudWatt.Count("", "*")
	fmt.Println("Number of Item : ", count, "Total size : ", cloudWatt.SumSize())

	log.Println("********************* START workflow Flexeengine ********************************** ")
	workflow(t, Flexeengine)
	log.Println("********************* START workflow ovh ********************************** ")
	workflow(t, ovh)
	log.Println("********************* START workflow cloudWatt ********************************** ")
	workflow(t, cloudWatt)
}

func workflow(t *testing.T, client *apistow.Location) {
	defer timeTrack(time.Now(), "********************* END workflow ")
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
	pattern := "*isfortes*"
	myContainerTest := "testpc"
	myItemTest1 := "ItemtestONE"
	myItemTest2 := "ItemtestTWO"
	myItemTest3 := "ItemtestTHREE"
	myItemTestSplit := "ItemtestSplitted"
	/*
		for j := 1; j <= 100; j++ {
			myContainerTest := "testpc" + strconv.Itoa(j)
			//err = client.Create(myContainerTest)
			err = client.Remove(myContainerTest)
		}
	*/
	lContainersItemsDeb, err := client.Inspect()
	/*affichRes(client, lContainersItemsDeb)*/

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
	assert.Nil(t, err)
	assert.Equal(t, size, sizefile)
	assert.Nil(t, err)
	f, sizefile, err = createAndReadforTest(fileNameTest)
	assert.Nil(t, err)
	err = client.PutItem(myContainerTest, myItemTest3, f, metadata)
	assert.Nil(t, err)
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
	//tim, err := client.ItemLastMod(myContainerTest, myItemTestSplit)
	err = client.WaitAllPutITemTerminated("user", pattern)
	assert.Nil(t, err)
	err = client.Clear(myContainerTest)
	assert.Nil(t, err)
	err = client.WaitAllPutITemTerminated("user", pattern)
	assert.Nil(t, err)
	pattern = "*isfortes*"
	lContainersItemsFiltered, err = client.FilterByMetadata("user", pattern)
	assert.Nil(t, err)
	assert.Empty(t, lContainersItemsFiltered)
	err = client.WaitAllPutITemTerminated("user", pattern)
	assert.Nil(t, err)
	err = client.Remove(myContainerTest)
	assert.Nil(t, err)
	err = client.WaitAllPutITemTerminated("user", pattern)
	assert.Nil(t, err)
	lContainersItemsFin, err := client.Inspect()
	assert.Nil(t, err)
	err = client.WaitAllPutITemTerminated("user", pattern)
	assert.Nil(t, err)
	//affichRes(client, lContainersItemsFin)
	eq := reflect.DeepEqual(lContainersItemsDeb, lContainersItemsFin)
	assert.True(t, eq)

}

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("%s took %s", name, elapsed)
}

func affichRes(client *apistow.Location, msoftabs map[string][]string) {
	for k, v := range msoftabs {
		fmt.Printf("Container : %s \n", k)
		for l := range v {
			if v[l] != "" {
				sizeIt, err := client.ItemSize(k, v[l])
				meta, err := client.ItemMetadata(k, v[l])
				if err != nil {
					fmt.Println(err)
				}
				fmt.Printf(" Items  %s Size : %d  Metadata : %s \n", v[l], sizeIt, meta)
			} else {
				fmt.Printf(" Container empty no item  \n")
			}
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

func worker(finished chan bool) {
	fmt.Println("Worker: Started")
	time.Sleep(5 * time.Second)
	fmt.Println("Worker: Finished")
	finished <- true
}
