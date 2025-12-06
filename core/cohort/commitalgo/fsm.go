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
	proposeStage   = "propose"
	precommitStage = "precommit"
	commitStage    = "commit"
)

type stateMachine struct {
	mu           sync.RWMutex
	currentState string
	mode         mode
	transitions  map[string]map[string]struct{}
}

var twoPhaseTransitions = map[string]map[string]struct{}{
	proposeStage: {
		proposeStage: struct{}{},
		commitStage:  struct{}{},
	},
	commitStage: {
		proposeStage: struct{}{},
	},
}

var threePhaseTransitions = map[string]map[string]struct{}{
	proposeStage: {
		proposeStage:   struct{}{},
		precommitStage: struct{}{},
	},
	precommitStage: {
		commitStage: struct{}{},
	},
	commitStage: {
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
