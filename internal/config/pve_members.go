package config

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
)

type pveMembersFile struct {
	NodeName string `json:"nodename"`
	Cluster  struct {
		Name string `json:"name"`
	} `json:"cluster"`
	NodeList map[string]struct {
		ID int    `json:"id"`
		IP string `json:"ip"`
	} `json:"nodelist"`
}

// PVEMembers contains cluster metadata loaded from /etc/pve/.members.
type PVEMembers struct {
	ClusterName string
	NodeName    string
	Nodes       []string
	// NodeIPs maps node name to its cluster IP. Used as the next hop for staged
	// host routes, which forward traffic to whichever node currently owns a VM.
	NodeIPs map[string]string
}

func readPVEMembers(path string) (PVEMembers, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return PVEMembers{}, fmt.Errorf("members path is empty")
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return PVEMembers{}, fmt.Errorf("reading %q: %w", path, err)
	}

	var raw pveMembersFile
	if err := json.Unmarshal(data, &raw); err != nil {
		return PVEMembers{}, fmt.Errorf("parsing %q: %w", path, err)
	}

	members := PVEMembers{
		ClusterName: strings.TrimSpace(raw.Cluster.Name),
		NodeName:    strings.TrimSpace(raw.NodeName),
		Nodes:       make([]string, 0, len(raw.NodeList)),
		NodeIPs:     make(map[string]string, len(raw.NodeList)),
	}

	type sortableNode struct {
		name string
		id   int
	}
	nodes := make([]sortableNode, 0, len(raw.NodeList))
	for name, node := range raw.NodeList {
		trimmed := strings.TrimSpace(name)
		if trimmed == "" {
			continue
		}
		nodes = append(nodes, sortableNode{name: trimmed, id: node.ID})
		if ip := strings.TrimSpace(node.IP); ip != "" {
			members.NodeIPs[trimmed] = ip
		}
	}
	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].id == nodes[j].id {
			return nodes[i].name < nodes[j].name
		}
		if nodes[i].id == 0 {
			return false
		}
		if nodes[j].id == 0 {
			return true
		}
		return nodes[i].id < nodes[j].id
	})
	for _, node := range nodes {
		members.Nodes = append(members.Nodes, node.name)
	}

	if members.ClusterName == "" {
		return PVEMembers{}, fmt.Errorf("missing cluster.name in %q", path)
	}
	if members.NodeName == "" {
		return PVEMembers{}, fmt.Errorf("missing nodename in %q", path)
	}
	if len(members.Nodes) == 0 {
		return PVEMembers{}, fmt.Errorf("missing nodelist in %q", path)
	}

	return members, nil
}

func (m PVEMembers) DefaultNATSServers() []string {
	servers := make([]string, 0, len(m.Nodes))
	for _, node := range m.Nodes {
		servers = append(servers, "tls://"+node+":4222")
	}
	return servers
}
