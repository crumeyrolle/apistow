package main

import (
	"fmt"
	"os"
	"time"

	"github.com/pcrume/apistow"
)

func main() {
	var myContainerName = "container-test-pc"       /*ATTENTION REGLE DE NOMMAGE CONTAINER FLEXIBLEENGINE */
	var myContainerNameBis = "container-test-pcbis" /*ATTENTION REGLE DE NOMMAGE CONTAINER FLEXIBLEENGINE */
	var metadata = make(map[string]interface{})
	var fileName = "/home/pierre/BigTIFF.tif"
	var fileNameforExtract = "/home/pierre/BigTIFF_extracted_from_cloud.tif"

	listContainers := make([]string, 0)
	listContainersItems := make([]string, 0)
	timed := time.Now()
	formatedTime := timed.Format(time.RFC1123)
	metadata["time"] = formatedTime
	metadata["company"] = "CS"
	metadata["user"] = "Crumeyrolle"
	metadata["country"] = "France"
	metadata["Split"] = ""

	fmt.Println(" ------------ debut test main -----------------")
	fmt.Println(" ------------ apistow.Connect -----------------")
	client := new(apistow.Location)
	client.Connect("OVH", "OVH")

	fmt.Println(" ------------  Inspect ------------")
	lContainersItems, err1 := client.Inspect()
	affichRes(client, lContainersItems)

	pattern1 := "*rumey*"
	key := "user"
	fmt.Printf("******* FilterByMetadata with key %s and pattern %s \n", key, pattern1)
	lContainersItems, err := client.FilterByMetadata(key, pattern1)
	affichRes(client, lContainersItems)

	fmt.Println("**************** ListItems****************")
	listItems, err := client.ListItems(myContainerName)
	affichRes(client, listItems)

	pattern1 = "*rumey*"
	key = "user"
	fmt.Printf("******* FilterItemsByMetadata with key %s and pattern %s \n", key, pattern1)
	lContainersItems, err = client.FilterItemsByMetadata(myContainerName, key, pattern1)
	affichRes(client, lContainersItems)
	return
	fmt.Println(" ------------ ListContainers ------------")
	listContainers, err = client.ListContainers()
	fmt.Println(listContainers)
	if err != nil {
		fmt.Println("erreur ListContainers ", err)
		return
	}
	fmt.Println("convert slice to map ListContainers ")

	elementMap := sliceToStrMap(listContainers)
	for k := range elementMap {
		fmt.Println("element map :", k)
	}

	pattern := "*rumey*"
	fmt.Printf("**************** Launch FilterByMetadata %s  \n", pattern)
	lContainersItems, err = client.FilterByMetadata("user", pattern)
	affichRes(client, lContainersItems)
	fmt.Println(listContainersItems)
	elementMap1 := sliceToStrMap(listContainersItems)
	for k := range elementMap1 {
		fmt.Println("element map1 :", k)
	}
	fmt.Println(elementMap1)

	fmt.Println("**************** ListItems****************")
	listItems, err = client.ListItems(myContainerName)
	affichRes(client, listItems)

	pattern1 = "*rumey*"
	key = "user"
	fmt.Printf("******* FilterItemsByMetadata with key %s and pattern %s \n", key, pattern1)
	lContainersItems, err = client.FilterItemsByMetadata(myContainerName, key, pattern1)
	affichRes(client, lContainersItems)

	myContainerTestContent := "testcontent"
	err1 = client.Create(myContainerTestContent)

	content := []byte("That's all folks!!")
	err = client.PutItemContent(myContainerTestContent, "newitem_testContent", content, metadata)
	fc, err := create("/home/pierre/chaine.txt")
	err = client.ExtractItem(myContainerTestContent, "newitem_testContent", fc, nil, nil)
	content, err = client.ExtractItemContent(myContainerTestContent, "newitem_testContent")
	fmt.Println("contenu de l'item : ", string(content[:]))
	return
	myContainerTest := "test"
	fmt.Printf("**************** Create Container  : %s \n", myContainerTest)
	err1 = client.Create(myContainerTest)
	f, err := read(fileName)
	err = client.PutItem(myContainerTest, "newitem7_testPC", f, metadata)
	f, err = create("/home/pierre/BigTIFF_Extracted.tif")
	err = client.ExtractItem(myContainerTest, "newitem7_testPC", f, nil, nil)

	//f, err = read("/home/pierre/BigTIFF_Extracted.tif")
	//err = client.PutItemByChunk(myContainerTest, "newitemSplitted", 100000000, f, metadata)

	f, err = create("/home/pierre/newitemSplitted0.tif")
	err = client.ExtractItem(myContainerName, "newitemSplitted0", f, nil, nil)
	sizeIt, err := client.ItemSize(myContainerName, "newitemSplitted0")
	fmt.Println("sizeIt sizeIt : ", sizeIt)
	fmt.Println("**************** ListItems****************")
	listItems, err = client.ListItems(myContainerTest)
	affichRes(client, listItems)

	//fmt.Printf("**************** Remove Container  : %s \n", myContainerTest)
	//err1 = client.Remove(myContainerTest)

	fmt.Printf("**************** Create Container for test : %s \n", myContainerName)
	err1 = client.Create(myContainerName)
	fmt.Printf("**************** Create Container for test : %s  \n", myContainerNameBis)
	err1 = client.Create(myContainerNameBis)

	fmt.Printf("**************** Launch Put Items in Container for test %s \n", myContainerName)
	f, err = read(fileName)
	err = client.PutItem(myContainerNameBis, "newitem7_testPC", f, metadata)
	return
	err = client.PutItem(myContainerName, "newitem1_testPC", f, metadata)
	err = client.PutItem(myContainerName, "newitem2_testPC", f, metadata)
	err = client.PutItem(myContainerName, "newitem3_testPC", f, metadata)
	err = client.PutItem(myContainerName, "newitem4_testPC", f, metadata)
	err = client.PutItem(myContainerName, "newitem5_testPC", f, metadata)
	err = client.PutItem(myContainerName, "newitem6_testPC", f, metadata)

	//client.PutSplitItembynbBytes(myContainerName, "newitemSplitted", 20, "/home/pierre/front.tar.gz", metadata)
	f, err = read("/home/pierre/1G.bin")
	err = client.PutItemByChunk(myContainerName, "newitemSplitted", 100000000, f, metadata)
	f, err = create("/home/pierre/newitemSplitted0.tif")
	err = client.ExtractItem(myContainerName, "newitemSplitted0", f, nil, nil)
	f, err = create("/home/pierre/newitemSplitted1.tif")
	err = client.ExtractItem(myContainerName, "newitemSplitted1", f, nil, nil)
	f, err = create("/home/pierre/newitemSplitted2.tif")
	err = client.ExtractItem(myContainerName, "newitemSplitted2", f, nil, nil)
	f, err = create("/home/pierre/newitemSplitted3.tif")
	err = client.ExtractItem(myContainerName, "newitemSplitted3", f, nil, nil)
	f, err = create("/home/pierre/newitemSplitted4.tif")
	err = client.ExtractItem(myContainerName, "newitemSplitted4", f, nil, nil)
	f, err = read("/home/pierre/frontAll.tar.gz")
	err = client.PutItem(myContainerName, "newitembig_testPC", f, metadata)
	seekto := int64(1000000000)
	length := int64(1000000000)
	f, err = create("/home/pierre/newitembig_testPC1.dat")
	err = client.ExtractItem(myContainerName, "newitembig_testPC", f, &seekto, &length)
	fmt.Println("ListAllContainersAndItems  ")
	listContainers, err = client.ListContainers()
	if err != nil {
		fmt.Println("erreur Liste Containers and Item   ")
	}
	for i := 0; i < len(listContainers); i++ {
		fmt.Printf("Container Name : %s \n", listContainers[i])
	}

	sizeIt, err = client.ItemSize(myContainerName, "newitembig_testPC")
	fmt.Println("sizeIt sizeIt : ", sizeIt)

	fmt.Println("**************** Metadata item  ****************")
	item, err1 := client.GetItem(myContainerName, "newitem6_testPC")
	if err1 != nil {
		fmt.Println("erreur GetItem : ", item.Name(), err1)
	}

	tag, err := client.ItemEtag(myContainerName, "newitembig_testPC")
	tim, err := client.ItemLastMod(myContainerName, "newitembig_testPC")
	size, err := client.ItemSize(myContainerName, "newitembig_testPC")
	id, err := client.ItemID(myContainerName, "newitembig_testPC")

	meta, err := client.ItemMetadata(myContainerName, "newitembig_testPC")

	fmt.Println(" tag de l'item => ", tag)
	fmt.Println(" time de l'item => ", tim)
	fmt.Println(" size de l'item => ", size)
	fmt.Println(" id de l'item => ", id)
	fmt.Println(" meta de l'item => ", meta)
	fmt.Println(" meta de l'item => ", meta["company"])

	fmt.Println("**************** Launch ExtractItem ****************")
	//f, err = create("/home/pierre/frontAll.tar.gz")
	//client.ExtractItem(myContainerName, "newitem6_testPC", f, nil, nil)
	f, err = create(fileNameforExtract)
	err = client.ExtractItem(myContainerName, "newitem6_testPC", f, nil, &size)
	/*
		if provider == "OVH" {
			var sizebyte int64 = 50
			// extract part of all big item
			client.ExtractItem("DEMO_CO3D", "docker/front.tar.gz", "/home/pierre/front.tar.gz", &sizebyte)
			// extract all big item
			//client.ExtractItem( "DEMO_CO3D", "docker/front.tar.gz", "/home/pierre/frontAll.tar.gz", nil)
			size, err = client.SizeItem("DEMO_CO3D", "docker/front.tar.gz")
		}*/
	f, err = create(fileNameforExtract)
	err = client.ExtractItem(myContainerName, "newitem6_testPC", f, nil, &size)

	fmt.Printf("**************** Launch search All Containers and Item  \n")
	listContainers, err = client.ListContainers()
	for i := 0; i < len(listContainers); i++ {
		fmt.Printf("Container Name : %s \n", listContainers[i])
	}
	pattern = "*rumey*"
	fmt.Printf("**************** Launch search  Containers and Items with pattern %s  \n", pattern)
	lContainersItems, err = client.FilterByMetadata("user", pattern)
	affichRes(client, lContainersItems)
	fmt.Printf("******* RESULTAT Launch search  Containers and Items with pattern %s  \n", pattern)
	affichResTAbString(client, listContainersItems)

	/*
		fmt.Println("**************** Remove Items and containers ****************")
		err = client.Clear(myContainerName)
		err = client.Clear(myContainerNameBis)
		fmt.Println("**************** Remove containers ", myContainerName, "****************")
		err1 = client.Remove(myContainerName)
		fmt.Println("**************** Remove containers ", myContainerNameBis, "****************")
		err1 = client.Remove(myContainerNameBis)
	*/
}

func sliceToStrMap(elements []string) map[string]string {
	elementMap := make(map[string]string)
	for _, s := range elements {
		elementMap[s] = s
	}
	return elementMap
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
func read(fileName string) (f *os.File, err error) {
	f, err = os.Open(fileName)
	if err != nil {
		fmt.Println(f, err)
		return f, err
	}
	return f, err
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

func affichResTAbString(client *apistow.Location, tabs []string) {

	for i := 0; i < len(tabs); i++ {
		if i%2 == 0 { // liste couple  container and item,
			sizeIt, err := client.ItemSize(tabs[i], tabs[i+1])
			if err != nil {
				fmt.Println(err)
			}
			container := tabs[i]
			item := tabs[i+1]
			fmt.Printf("Container Name : %s \t ItemName : %s  Size : %d \n", container, item, sizeIt)
			//fmt.Printf("Container Name : %s \t ItemName : %s \n", listContainersItems[i], listContainersItems[i+1])
		}
	}
}
