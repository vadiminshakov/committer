// This example moves money between accounts held by two different banks.
// Each bank is a participant with its own Resource: the debit side reserves
// funds in Prepare, so a transfer either completes at both banks or at none.
//
// The coordinator and both participants run in one process for brevity; in a
// real deployment each bank runs its participant next to its own database.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/vadiminshakov/committer/v2"
)

const (
	coordinatorAddr = "localhost:3100"
	northBankAddr   = "localhost:3101"
	southBankAddr   = "localhost:3102"

	alice = "alice"
	bob   = "bob"

	aliceBalance    = 100
	bobBalance      = 20
	affordableSum   = 30
	unaffordableSum = 500

	settlePollInterval = 10 * time.Millisecond
)

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

func (b *bank) Prepare(_ context.Context, txn committer.Tx) error {
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

func (b *bank) Commit(_ context.Context, txn committer.Tx) error {
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

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))

	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "Example failed:", err)
		os.Exit(1)
	}
}

func run() error {
	dataDir, err := os.MkdirTemp("", "committer-transfer-")
	if err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}
	defer os.RemoveAll(dataDir)

	north := newBank("north-bank", map[string]int{alice: aliceBalance})
	south := newBank("south-bank", map[string]int{bob: bobBalance})

	for addr, resource := range map[string]*bank{northBankAddr: north, southBankAddr: south} {
		participant, err := committer.StartParticipant(context.Background(), committer.ParticipantConfig{
			Addr:        addr,
			Coordinator: coordinatorAddr,
			DataDir:     dataDir,
		}, resource)
		if err != nil {
			return fmt.Errorf("start %s: %w", resource.name, err)
		}
		defer participant.Close()
	}

	coordinator, err := committer.StartCoordinator(committer.CoordinatorConfig{
		Addr:         coordinatorAddr,
		Participants: []string{northBankAddr, southBankAddr},
		DataDir:      dataDir,
	})
	if err != nil {
		return fmt.Errorf("start coordinator: %w", err)
	}
	defer coordinator.Close()

	for _, amount := range []int{affordableSum, unaffordableSum} {
		if err := send(coordinator, transfer{From: alice, To: bob, Amount: amount}); err != nil {
			return err
		}
	}

	// COMMIT reaches participants asynchronously; closing the coordinator
	// first would leave delivery to its next start.
	fmt.Println(north.settled())
	fmt.Println(south.settled())

	return nil
}

func send(coordinator *committer.Coordinator, move transfer) error {
	payload, err := json.Marshal(move)
	if err != nil {
		return fmt.Errorf("encode transfer: %w", err)
	}

	height, err := coordinator.Commit(context.Background(), "transfer", payload)

	switch {
	case errors.Is(err, committer.ErrAborted):
		fmt.Printf("transfer of %d aborted: %v\n", move.Amount, err)
	case err != nil:
		return fmt.Errorf("transfer of %d: %w", move.Amount, err)
	default:
		fmt.Printf("transfer of %d committed at height %d\n", move.Amount, height)
	}

	return nil
}
