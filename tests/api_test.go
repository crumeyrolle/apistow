package tests

import (
	"github.com/pcrume/apistow"
	"github.com/stretchr/testify/assert"

	"testing"
)

//ClientTester ClientTester
type ClientTester struct {
	apistow.ObjectStorageAPI
}

func TestConnect(t *testing.T) {
	client := new(apistow.Location)
	s1 := "OVH"
	s2 := "OVH"
	err := client.Connect(s1, s2)
	assert.Nil(t, err)

}
