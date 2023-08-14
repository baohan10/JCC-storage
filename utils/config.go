package utils

import (
	"fmt"
	"regexp"
	"strconv"

	"github.com/beevik/etree"
)

type EcConfig struct {
	ecid  string `xml:"ecid"`
	class string `xml:"class"`
	n     int    `xml:"n"`
	k     int    `xml:"k"`
	w     int    `xml:"w"`
	opt   int    `xml:"opt"`
}

func (r *EcConfig) GetK() int {
	return r.k
}

func (r *EcConfig) GetN() int {
	return r.n
}

func GetEcPolicy() *map[string]EcConfig {
	doc := etree.NewDocument()
	if err := doc.ReadFromFile("../conf/sysSetting.xml"); err != nil {
		panic(err)
	}
	ecMap := make(map[string]EcConfig, 20)
	root := doc.SelectElement("setting")
	for _, attr := range root.SelectElements("attribute") {
		if name := attr.SelectElement("name"); name.Text() == "ec.policy" {
			for _, eci := range attr.SelectElements("value") {
				tt := EcConfig{}
				tt.ecid = eci.SelectElement("ecid").Text()
				tt.class = eci.SelectElement("class").Text()
				tt.n, _ = strconv.Atoi(eci.SelectElement("n").Text())
				tt.k, _ = strconv.Atoi(eci.SelectElement("k").Text())
				tt.w, _ = strconv.Atoi(eci.SelectElement("w").Text())
				tt.opt, _ = strconv.Atoi(eci.SelectElement("opt").Text())
				ecMap[tt.ecid] = tt
			}
		}
	}
	fmt.Println(ecMap)
	return &ecMap
	//
}

func GetAgentIps() []string {
	doc := etree.NewDocument()
	if err := doc.ReadFromFile("../conf/sysSetting.xml"); err != nil {
		panic(err)
	}
	root := doc.SelectElement("setting")
	var ips []string // 定义存储 IP 的字符串切片

	for _, attr := range root.SelectElements("attribute") {
		if name := attr.SelectElement("name"); name.Text() == "agents.addr" {
			for _, ip := range attr.SelectElements("value") {
				ipRegex := regexp.MustCompile(`\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b`)
				match := ipRegex.FindString(ip.Text())
				print(match)
				ips = append(ips, match)
			}
		}
	}

	return ips
}
