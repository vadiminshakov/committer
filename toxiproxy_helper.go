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

type ToxiproxyClient struct {
	BaseURL    string
	HTTPClient *http.Client
}

type Proxy struct {
	Name     string `json:"name"`
	Listen   string `json:"listen"`
	Upstream string `json:"upstream"`
	Enabled  bool   `json:"enabled"`
}

type Toxic struct {
	Name       string                 `json:"name"`
	Type       string                 `json:"type"`
	Stream     string                 `json:"stream"`
	Toxicity   float32                `json:"toxicity"`
	Attributes map[string]interface{} `json:"attributes"`
}

func NewToxiproxyClient(baseURL string) *ToxiproxyClient {
	return &ToxiproxyClient{
		BaseURL:    baseURL,
		HTTPClient: &http.Client{Timeout: 10 * time.Second},
	}
}

func (c *ToxiproxyClient) CreateProxy(name, listen, upstream string) (*Proxy, error) {
	proxy := &Proxy{
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

	var createdProxy Proxy
	if err := json.Unmarshal(body, &createdProxy); err != nil {
		return nil, fmt.Errorf("failed to parse created proxy response: %v, body: %s", err, string(body))
	}

	return &createdProxy, nil
}

func (c *ToxiproxyClient) AddToxic(proxyName string, toxic *Toxic) error {
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

func (c *ToxiproxyClient) DeleteProxy(name string) error {
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

func (c *ToxiproxyClient) ResetProxy(name string) error {
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

type ChaosTestHelper struct {
	client       *ToxiproxyClient
	proxies      map[string]*Proxy
	proxyMapping map[string]string // original -> proxy address
}

func newChaosTestHelper(toxiproxyURL string) *ChaosTestHelper {
	return &ChaosTestHelper{
		client:       NewToxiproxyClient(toxiproxyURL),
		proxies:      make(map[string]*Proxy),
		proxyMapping: make(map[string]string),
	}
}

func (h *ChaosTestHelper) SetupProxies(nodeAddresses []string) error {
	for i, addr := range nodeAddresses {
		proxyName := fmt.Sprintf("node_%d", i)
		proxyAddr := h.generateProxyAddress(addr)

		proxy, err := h.client.CreateProxy(proxyName, proxyAddr, addr)
		if err != nil {
			return fmt.Errorf("failed to create proxy for %s: %v", addr, err)
		}

		h.proxies[proxyName] = proxy
		h.proxyMapping[addr] = proxyAddr
	}

	return nil
}

func (h *ChaosTestHelper) AddLatency(nodeAddr string, latency time.Duration) error {
	proxyName := h.getProxyName(nodeAddr)
	if proxyName == "" {
		return fmt.Errorf("proxy not found for address %s", nodeAddr)
	}

	toxic := &Toxic{
		Name:     fmt.Sprintf("latency_%s", proxyName),
		Type:     "latency",
		Stream:   "downstream",
		Toxicity: 1.0,
		Attributes: map[string]interface{}{
			"latency": int(latency.Milliseconds()),
			"jitter":  0,
		},
	}

	return h.client.AddToxic(proxyName, toxic)
}

func (h *ChaosTestHelper) AddTimeout(nodeAddr string, timeout time.Duration) error {
	proxyName := h.getProxyName(nodeAddr)
	if proxyName == "" {
		return fmt.Errorf("proxy not found for address %s", nodeAddr)
	}

	toxic := &Toxic{
		Name:     fmt.Sprintf("timeout_%s", proxyName),
		Type:     "timeout",
		Stream:   "downstream",
		Toxicity: 1.0,
		Attributes: map[string]interface{}{
			"timeout": int(timeout.Milliseconds()),
		},
	}

	return h.client.AddToxic(proxyName, toxic)
}

func (h *ChaosTestHelper) AddBandwidthLimit(nodeAddr string, rate int) error {
	proxyName := h.getProxyName(nodeAddr)
	if proxyName == "" {
		return fmt.Errorf("proxy not found for address %s", nodeAddr)
	}

	toxic := &Toxic{
		Name:     fmt.Sprintf("bandwidth_%s", proxyName),
		Type:     "bandwidth",
		Stream:   "downstream",
		Toxicity: 1.0,
		Attributes: map[string]interface{}{
			"rate": rate,
		},
	}

	return h.client.AddToxic(proxyName, toxic)
}

// AddResetPeer simulates TCP RESET after optional timeout
func (h *ChaosTestHelper) AddResetPeer(nodeAddr string, timeout time.Duration) error {
	proxyName := h.getProxyName(nodeAddr)
	if proxyName == "" {
		return fmt.Errorf("proxy not found for address %s", nodeAddr)
	}

	toxic := &Toxic{
		Name:     fmt.Sprintf("reset_peer_%s", proxyName),
		Type:     "reset_peer",
		Stream:   "downstream",
		Toxicity: 1.0,
		Attributes: map[string]interface{}{
			"timeout": int(timeout.Milliseconds()),
		},
	}

	return h.client.AddToxic(proxyName, toxic)
}

// AddDataLimit closes connection after transmitting specified bytes
func (h *ChaosTestHelper) AddDataLimit(nodeAddr string, bytes int) error {
	proxyName := h.getProxyName(nodeAddr)
	if proxyName == "" {
		return fmt.Errorf("proxy not found for address %s", nodeAddr)
	}

	toxic := &Toxic{
		Name:     fmt.Sprintf("limit_data_%s", proxyName),
		Type:     "limit_data",
		Stream:   "downstream",
		Toxicity: 1.0,
		Attributes: map[string]interface{}{
			"bytes": bytes,
		},
	}

	return h.client.AddToxic(proxyName, toxic)
}

// AddSlicer slices TCP data into small packets with optional delay
func (h *ChaosTestHelper) AddSlicer(nodeAddr string, avgSize, sizeVariation int, delay time.Duration) error {
	proxyName := h.getProxyName(nodeAddr)
	if proxyName == "" {
		return fmt.Errorf("proxy not found for address %s", nodeAddr)
	}

	toxic := &Toxic{
		Name:     fmt.Sprintf("slicer_%s", proxyName),
		Type:     "slicer",
		Stream:   "downstream",
		Toxicity: 1.0,
		Attributes: map[string]interface{}{
			"average_size":   avgSize,
			"size_variation": sizeVariation,
			"delay":          int(delay.Microseconds()),
		},
	}

	return h.client.AddToxic(proxyName, toxic)
}

func (h *ChaosTestHelper) GetProxyAddress(originalAddr string) string {
	return h.proxyMapping[originalAddr]
}

func (h *ChaosTestHelper) ResetChaos(nodeAddr string) error {
	proxyName := h.getProxyName(nodeAddr)
	if proxyName == "" {
		return fmt.Errorf("proxy not found for address %s", nodeAddr)
	}

	return h.client.ResetProxy(proxyName)
}

func (h *ChaosTestHelper) Cleanup() error {
	for proxyName := range h.proxies {
		if err := h.client.DeleteProxy(proxyName); err != nil {
			return err
		}
	}

	h.proxies = make(map[string]*Proxy)
	h.proxyMapping = make(map[string]string)
	return nil
}

func (h *ChaosTestHelper) generateProxyAddress(originalAddr string) string {
	parts := strings.Split(originalAddr, ":")
	if len(parts) != 2 {
		return originalAddr // fallback
	}

	port, err := strconv.Atoi(parts[1])
	if err != nil {
		return originalAddr // fallback
	}

	// добавляем 10000 к порту для прокси
	proxyPort := port + 10000
	return fmt.Sprintf("%s:%d", parts[0], proxyPort)
}

func (h *ChaosTestHelper) getProxyName(nodeAddr string) string {
	for name, proxy := range h.proxies {
		if proxy.Upstream == nodeAddr {
			return name
		}
	}
	return ""
}
