package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/netip"

	"github.com/phoban01/cask/internal/transport/nebula"
)

// enroll POSTs to a `cask mint` endpoint and builds the node's Nebula config in
// memory from the response. It returns the config YAML, whether the node is
// client-only (role=="client"), and the discovery SRV name the minter advertised
// (empty if none). The cert is held only in memory for this process — a restart
// re-enrolls as a fresh identity (the trade-off of stateless, allocation-free
// minting).
func enroll(ctx context.Context, mintURL, token, zone, role string) (configYAML string, clientOnly bool, discoverySRV string, err error) {
	body, _ := json.Marshal(mintReq{Token: token, Zone: zone, Role: role})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, mintURL+"/mint", bytes.NewReader(body))
	if err != nil {
		return "", false, "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", false, "", fmt.Errorf("enroll request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", false, "", fmt.Errorf("enroll: mint returned %s", resp.Status)
	}
	var mr mintResp
	if err := json.NewDecoder(resp.Body).Decode(&mr); err != nil {
		return "", false, "", fmt.Errorf("enroll decode: %w", err)
	}
	ip, err := netip.ParseAddr(mr.OverlayIP)
	if err != nil {
		return "", false, "", fmt.Errorf("enroll: bad overlay_ip %q: %w", mr.OverlayIP, err)
	}
	spec := nebula.NodeSpec{
		Name:      mr.Name,
		OverlayIP: ip,
		Zone:      zone,
		UDP:       "0.0.0.0:4242",
		// not a lighthouse; rendezvous via the minted lighthouse hosts.
	}
	cfg, err := nebula.BuildNebulaConfig(spec, []byte(mr.CA), []byte(mr.Cert), []byte(mr.Key), mr.LighthouseHosts, mr.StaticHostMap)
	if err != nil {
		return "", false, "", fmt.Errorf("enroll build config: %w", err)
	}
	return cfg, mr.Role == "client", mr.DiscoverySRV, nil
}
