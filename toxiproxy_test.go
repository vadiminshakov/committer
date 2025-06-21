//go:build chaos

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type toxiproxyClient struct {
	BaseURL    string
	HTTPClient *http.Client
}

type proxy struct {
	Name     string `json:"name"`
	Listen   string `json:"listen"`
	Upstream string `json:"upstream"`
	Enabled  bool   `json:"enabled"`
}

type toxic struct {
	Name       string                 `json:"name"`
	Type       string                 `json:"type"`
	Stream     string                 `json:"stream"`
	Toxicity   float32                `json:"toxicity"`
	Attributes map[string]interface{} `json:"attributes"`
}

func newToxiproxyClient(baseURL string) *toxiproxyClient {
	return &toxiproxyClient{
		BaseURL:    baseURL,
		HTTPClient: &http.Client{Timeout: 10 * time.Second},
	}
}

func (c *toxiproxyClient) createProxy(name, listen, upstream string) (*proxy, error) {
	proxy := &proxy{
		Name:     name,
		Listen:   listen,
		Upstream: upstream,
		Enabled:  true,
	}

	data, err := json.Marshal(proxy)
	if err != nil {
		return nil, err
	}

	resp, err := c.HTTPClient.Post(c.BaseURL+"/proxies", "application/json", bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("failed to create proxy (status %d): %s", resp.StatusCode, string(body))
	}

	var createdProxy proxy
	if err := json.Unmarshal(body, &createdProxy); err != nil {
		return nil, fmt.Errorf("failed to parse created proxy response: %v, body: %s", err, string(body))
	}

	return &createdProxy, nil
}

func (c *toxiproxyClient) addToxic(proxyName string, toxic *toxic) error {
	data, err := json.Marshal(toxic)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("%s/proxies/%s/toxics", c.BaseURL, proxyName)
	resp, err := c.HTTPClient.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to add toxic to proxy '%s' (status %d): %s. Request: %s", proxyName, resp.StatusCode, string(body), string(data))
	}

	return nil
}

func (c *toxiproxyClient) deleteProxy(name string) error {
	url := fmt.Sprintf("%s/proxies/%s", c.BaseURL, name)
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return err
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to delete proxy: %s", string(body))
	}

	return nil
}

func (c *toxiproxyClient) resetProxy(name string) error {
	url := fmt.Sprintf("%s/proxies/%s/toxics", c.BaseURL, name)
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return err
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}

type chaosTestHelper struct {
	client       *toxiproxyClient
	proxies      map[string]*proxy
	proxyMapping map[string]string // original -> proxy address
}

func newChaosTestHelper(toxiproxyURL string) *chaosTestHelper {
	return &chaosTestHelper{
		client:       newToxiproxyClient(toxiproxyURL),
		proxies:      make(map[string]*proxy),
		proxyMapping: make(map[string]string),
	}
}

func (h *chaosTestHelper) setupProxies(nodeAddresses []string) error {
	for i, addr := range nodeAddresses {
		proxyName := fmt.Sprintf("node_%d", i)
		proxyAddr := h.generateProxyAddress(addr)

		proxy, err := h.client.createProxy(proxyName, proxyAddr, addr)
		if err != nil {
			return fmt.Errorf("failed to create proxy for %s: %v", addr, err)
		}

		h.proxies[proxyName] = proxy
		h.proxyMapping[addr] = proxyAddr
	}

	return nil
}

// addResetPeer simulates TCP RESET after optional timeout
func (h *chaosTestHelper) addResetPeer(nodeAddr string, timeout time.Duration) error {
	proxyName := h.getProxyName(nodeAddr)
	if proxyName == "" {
		return fmt.Errorf("proxy not found for address %s", nodeAddr)
	}

	toxic := &toxic{
		Name:     fmt.Sprintf("reset_peer_%s", proxyName),
		Type:     "reset_peer",
		Stream:   "downstream",
		Toxicity: 1.0,
		Attributes: map[string]interface{}{
			"timeout": int(timeout.Milliseconds()),
		},
	}

	return h.client.addToxic(proxyName, toxic)
}

// addDataLimit closes connection after transmitting specified bytes
func (h *chaosTestHelper) addDataLimit(nodeAddr string, bytes int) error {
	proxyName := h.getProxyName(nodeAddr)
	if proxyName == "" {
		return fmt.Errorf("proxy not found for address %s", nodeAddr)
	}

	toxic := &toxic{
		Name:     fmt.Sprintf("limit_data_%s", proxyName),
		Type:     "limit_data",
		Stream:   "downstream",
		Toxicity: 1.0,
		Attributes: map[string]interface{}{
			"bytes": bytes,
		},
	}

	return h.client.addToxic(proxyName, toxic)
}

func (h *chaosTestHelper) getProxyAddress(originalAddr string) string {
	return h.proxyMapping[originalAddr]
}

func (h *chaosTestHelper) cleanup() error {
	for proxyName := range h.proxies {
		if err := h.client.deleteProxy(proxyName); err != nil {
			return err
		}
	}

	h.proxies = make(map[string]*proxy)
	h.proxyMapping = make(map[string]string)
	return nil
}

func (h *chaosTestHelper) generateProxyAddress(originalAddr string) string {
	parts := strings.Split(originalAddr, ":")
	if len(parts) != 2 {
		return originalAddr
	}

	port, err := strconv.Atoi(parts[1])
	if err != nil {
		return originalAddr
	}

	proxyPort := port + 10000
	return fmt.Sprintf("%s:%d", parts[0], proxyPort)
}

func (h *chaosTestHelper) getProxyName(nodeAddr string) string {
	for name, proxy := range h.proxies {
		if proxy.Upstream == nodeAddr {
			return name
		}
	}
	return ""
}
