package commitalgo

import (
	"errors"
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
	currentState string
	mode         mode
	transitions  map[string]map[string]struct{}
}

var twoPhaseTransitions = map[string]map[string]struct{}{
	proposeStage: {
		proposeStage: struct{}{},
		commitStage:  struct{}{},
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
	if allowedStates, ok := sm.transitions[sm.currentState]; ok {
		if _, ok = allowedStates[nextState]; ok {
			sm.currentState = nextState
			return nil
		}
	}

	return errors.New("invalid state transition")
}
