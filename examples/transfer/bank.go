package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/vadiminshakov/committer/v2/core/dto"
)

const settlePollInterval = 10 * time.Millisecond

// transfer is the transaction payload.
type transfer struct {
	From   string `json:"from"`
	To     string `json:"to"`
	Amount int    `json:"amount"`
}

// bank keeps balances in memory. Prepare holds the debited amount until the
// coordinator's decision arrives.
type bank struct {
	name     string
	mu       sync.Mutex
	balances map[string]int
	holds    map[uint64]transfer // prepared transfers by height
	applied  map[uint64]bool     // makes Commit idempotent
}

func newBank(name string, balances map[string]int) *bank {
	return &bank{name: name, balances: balances, holds: map[uint64]transfer{}, applied: map[uint64]bool{}}
}

func (b *bank) Prepare(_ context.Context, txn dto.Tx) error {
	var move transfer
	if err := json.Unmarshal(txn.Value, &move); err != nil {
		return fmt.Errorf("%s: bad transfer: %w", b.name, err)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.balances[move.From]; ok && b.available(move.From) < move.Amount {
		return fmt.Errorf("%s: %s has insufficient funds", b.name, move.From)
	}

	b.holds[txn.Height] = move

	return nil
}

func (b *bank) Commit(_ context.Context, txn dto.Tx) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.applied[txn.Height] {
		return nil
	}

	var move transfer
	if err := json.Unmarshal(txn.Value, &move); err != nil {
		return fmt.Errorf("%s: bad transfer: %w", b.name, err)
	}

	if _, ok := b.balances[move.From]; ok {
		b.balances[move.From] -= move.Amount
	}

	if _, ok := b.balances[move.To]; ok {
		b.balances[move.To] += move.Amount
	}

	delete(b.holds, txn.Height)
	b.applied[txn.Height] = true

	return nil
}

func (b *bank) Abort(_ context.Context, height uint64) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	delete(b.holds, height)

	return nil
}

// available is the balance minus funds held by prepared transfers.
func (b *bank) available(account string) int {
	amount := b.balances[account]
	for _, held := range b.holds {
		if held.From == account {
			amount -= held.Amount
		}
	}

	return amount
}

// settled returns the balances once the bank holds no prepared transfer.
func (b *bank) settled() string {
	for {
		b.mu.Lock()
		done := len(b.holds) == 0 && len(b.applied) > 0
		summary := fmt.Sprintf("%s %v", b.name, b.balances)
		b.mu.Unlock()

		if done {
			return summary
		}

		time.Sleep(settlePollInterval)
	}
}
