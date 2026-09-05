package discovery

import (
	"context"
	"encoding/json"
	"os/exec"
)

type status struct {
	Peer map[string]*TailscalePeer `json:"Peer"`
}

type TailscalePeer struct {
	HostName     string   `json:"HostName"`
	TailscaleIPs []string `json:"TailscaleIPs"`
	OS           string   `json:"OS"`
	Online       bool     `json:"Online"`
}

func parseRawOutput(output []byte) ([]*TailscalePeer, error) {
	var st status
	err := json.Unmarshal(output, &st)
	if err != nil {
		return nil, err
	}

	var peers []*TailscalePeer
	for _, peer := range st.Peer {
		peers = append(peers, peer)
	}

	return peers, nil
}

func GetPeers(ctx context.Context) ([]*TailscalePeer, error) {
	cmd := exec.CommandContext(ctx, "tailscale", "status", "--json")

	output, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	peers, err := parseRawOutput(output)
	if err != nil {
		return nil, err
	}

	return peers, nil
}
