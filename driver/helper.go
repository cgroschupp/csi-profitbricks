package driver

import (
  "github.com/profitbricks/profitbricks-sdk-go"
  "io/ioutil"
  "strings"
)

const (
	productUUIDPath = "/sys/devices/virtual/dmi/id/product_uuid"
)

func choose(ss []profitbricks.Volume, test func(profitbricks.Volume) bool) (ret []profitbricks.Volume) {
    for _, s := range ss {
        if test(s) {
            ret = append(ret, s)
        }
    }
    return
}

func GetServerID() (string, error) {
	output, err := ioutil.ReadFile(productUUIDPath)
	toReturn := string(output)
	return strings.TrimSpace(toReturn), err
}
