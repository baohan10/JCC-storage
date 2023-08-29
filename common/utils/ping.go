package utils

import (
	//"fmt"
	"github.com/go-ping/ping"
	//"net"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

type ConnStatus struct {
	Addr        string
	IsReachable bool
	Delay       time.Duration
	TTL         int
}

// 获取本地主机 IP 地址
func getLocalIP() string {
	resp, err := http.Get("https://api.ipify.org")
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}

	ip := strings.TrimSpace(string(body))
	return ip
}

func GetConnStatus(remoteIP string) (*ConnStatus, error) {
	// 本地主机 IP 地址
	//localIP := getLocalIP()
	//print("!@#@#!")
	//print(localIP)
	conn := ConnStatus{
		Addr:        remoteIP,
		IsReachable: false,
	}
	pinger, err := ping.NewPinger(remoteIP)

	if err != nil {
		return nil, err
	}
	pinger.Count = 5 // 设置 ping 次数为 5
	// pinger.Interval = 1 // 设置 ping 时间间隔为 1 秒
	//pinger.Timeout = 2  // 设置 ping 超时时间为 2 秒
	//pinger.SetPrivileged(true) // 设置使用特权模式以获取 TTL 值
	pinger.OnRecv = func(pkt *ping.Packet) {
		//fmt.Printf("%d bytes from %s: icmp_seq=%d time=%v ttl=%v (DUP!)\n",
		//	pkt.Nbytes, pkt.IPAddr, pkt.Seq, pkt.Rtt, pkt.Ttl)
		conn.TTL = pkt.Ttl
	}

	/*pinger.OnDuplicateRecv = func(pkt *ping.Packet) {
		fmt.Printf("%d bytes from %s: icmp_seq=%d time=%v ttl=%v (DUP!)\n",
			pkt.Nbytes, pkt.IPAddr, pkt.Seq, pkt.Rtt, pkt.Ttl)
	}*/

	pinger.OnFinish = func(stats *ping.Statistics) {
		//fmt.Printf("\n--- %s ping statistics ---\n", stats.Addr)
		//fmt.Printf("%d packets transmitted, %d packets received, %v%% packet loss\n",
		//  stats.PacketsSent, stats.PacketsRecv, stats.PacketLoss)
		//fmt.Printf("round-trip min/avg/max/stddev = %v/%v/%v/%v\n",
		//	stats.MinRtt, stats.AvgRtt, stats.MaxRtt, stats.StdDevRtt)
		if stats.PacketLoss == 0.0 {
			conn.IsReachable = true
		}
		conn.Delay = stats.AvgRtt
	}
	err = pinger.Run() // Blocks until finished.
	if err != nil {
		return nil, err
	}
	return &conn, nil
}
