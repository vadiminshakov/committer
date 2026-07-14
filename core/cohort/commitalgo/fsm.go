package commitalgo

import (
	"errors"
	"sync"
)

type mode string

const (
	twophase   = "two-phase"
	threephase = "three-phase"
)
const (
	// proposeStage means that the cohort is ready to accept a transaction at
	// the current height.
	proposeStage = "propose"
	// preparedStage is the PREPARED/WAITING state. The cohort has voted YES,
	// durably stored the payload, and must wait for the coordinator's next
	// decision. In 3PC this is deliberately distinct from proposeStage.
	preparedStage = "prepared"
	// precommitStage is committable: the cohort must not choose ABORT locally.
	precommitStage = "precommit"
	// commitStage is the short-lived state while the final commit is being
	// applied. The protocol returns to proposeStage only after the final WAL
	// and store operations succeed.
	commitStage = "commit"
)

type stateMachine struct {
	mu           sync.RWMutex
	currentState string
	mode         mode
	transitions  map[string]map[string]struct{}
}

// In 2PC a cohort that voted YES enters the prepared state (uncertainty
// period) and leaves it only on a commit or abort decision.
var twoPhaseTransitions = map[string]map[string]struct{}{
	proposeStage: {
		proposeStage:  struct{}{},
		preparedStage: struct{}{},
	},
	preparedStage: {
		preparedStage: struct{}{},
		commitStage:   struct{}{},
		proposeStage:  struct{}{}, // abort
	},
	commitStage: {
		commitStage:  struct{}{},
		proposeStage: struct{}{},
	},
}

var threePhaseTransitions = map[string]map[string]struct{}{
	proposeStage: {
		proposeStage:  struct{}{},
		preparedStage: struct{}{},
	},
	preparedStage: {
		preparedStage:  struct{}{},
		precommitStage: struct{}{},
		proposeStage:   struct{}{}, // coordinator ABORT before PRECOMMIT
	},
	precommitStage: {
		precommitStage: struct{}{},
		commitStage:    struct{}{},
	},
	commitStage: {
		commitStage:  struct{}{},
		proposeStage: struct{}{},
	},
}

func newStateMachine(mode mode) *stateMachine {
	tr := twoPhaseTransitions
	if mode == threephase {
		tr = threePhaseTransitions
	}

	return &stateMachine{
		currentState: proposeStage,
		mode:         mode,
		transitions:  tr,
	}
}

func (sm *stateMachine) Transition(nextState string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if allowedStates, ok := sm.transitions[sm.currentState]; ok {
		if _, ok = allowedStates[nextState]; ok {
			sm.currentState = nextState
			return nil
		}
	}

	return errors.New("invalid state transition")
}

func (sm *stateMachine) getCurrentState() string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.currentState
}

func (sm *stateMachine) GetMode() mode {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.mode
}
