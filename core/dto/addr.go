package dto

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

// Addr is a node address in host:port form.
type Addr string

func (a Addr) Validate() error {
	host, port, err := net.SplitHostPort(string(a))
	if err != nil || strings.TrimSpace(host) == "" || strings.ContainsAny(host, " /\\\t\n") {
		return fmt.Errorf("invalid address %q: expected host:port", a)
	}

	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return fmt.Errorf("invalid port in %q: expected 1–65535", a)
	}

	return nil
}

func (a Addr) String() string {
	return string(a)
}
