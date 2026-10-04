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

	"github.com/vadiminshakov/committer/v2/core/cohort"
	"github.com/vadiminshakov/committer/v2/core/coordinator"
	"github.com/vadiminshakov/committer/v2/core/dto"
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
)

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

	for addr, resource := range map[dto.Addr]*bank{northBankAddr: north, southBankAddr: south} {
		participant, err := cohort.Start(context.Background(), cohort.Config{
			Addr:        addr,
			Coordinator: coordinatorAddr,
			DataDir:     dataDir,
		}, resource)
		if err != nil {
			return fmt.Errorf("start %s: %w", resource.name, err)
		}
		defer participant.Close()
	}

	coord, err := coordinator.Start(coordinator.Config{
		Addr:    coordinatorAddr,
		Cohorts: []dto.Addr{northBankAddr, southBankAddr},
		DataDir: dataDir,
	})
	if err != nil {
		return fmt.Errorf("start coordinator: %w", err)
	}
	defer coord.Close()

	for _, amount := range []int{affordableSum, unaffordableSum} {
		if err := send(coord, transfer{From: alice, To: bob, Amount: amount}); err != nil {
			return err
		}
	}

	// COMMIT reaches participants asynchronously; closing the coordinator
	// first would leave delivery to its next start.
	fmt.Println(north.settled())
	fmt.Println(south.settled())

	return nil
}

func send(coord *coordinator.Coordinator, move transfer) error {
	payload, err := json.Marshal(move)
	if err != nil {
		return fmt.Errorf("encode transfer: %w", err)
	}

	height, err := coord.Commit(context.Background(), "transfer", payload)

	switch {
	case errors.Is(err, coordinator.ErrAborted):
		fmt.Printf("transfer of %d aborted: %v\n", move.Amount, err)
	case err != nil:
		return fmt.Errorf("transfer of %d: %w", move.Amount, err)
	default:
		fmt.Printf("transfer of %d committed at height %d\n", move.Amount, height)
	}

	return nil
}
