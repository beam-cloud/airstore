package cli

import (
	"context"
	"fmt"
	"time"

	"github.com/charmbracelet/huh/spinner"
)

// SpinnerAction represents a step with a spinner
type SpinnerAction struct {
	Title      string
	SuccessMsg string
	SuccessVal string // Optional value to show after success
	Action     func() error
	ActionCtx  func(ctx context.Context) error
}

// RunSpinner runs a single action with a spinner
func RunSpinner(title string, fn func() error) error {
	return spinner.New().
		Title(title).
		Action(func() {
			// Wrap to handle the error separately
		}).
		Run()
}

// RunSpinnerWithResult runs an action with a spinner and returns any error
func RunSpinnerWithResult(title string, fn func() error) error {
	var actionErr error

	err := spinner.New().
		Title("  " + title).
		Action(func() {
			actionErr = fn()
		}).
		Run()

	if err != nil {
		return err
	}
	return actionErr
}

// RunSpinnerCtx runs an action with a spinner and context
func RunSpinnerCtx(ctx context.Context, title string, fn func(ctx context.Context) error) error {
	var actionErr error

	err := spinner.New().
		Title("  " + title).
		Action(func() {
			actionErr = fn(ctx)
		}).
		Run()

	if err != nil {
		return err
	}
	return actionErr
}

// SpinnerStep represents a step in a multi-step process
type SpinnerStep struct {
	Title      string
	SuccessMsg string
	Value      string // Optional value to display on success
	Run        func() error
}

// RunSteps executes a series of steps with spinners, showing success after each
func RunSteps(steps []SpinnerStep) error {
	for _, step := range steps {
		var actionErr error

		err := spinner.New().
			Title("  " + step.Title).
			Action(func() {
				actionErr = step.Run()
			}).
			Run()

		if err != nil {
			return err
		}
		if actionErr != nil {
			return actionErr
		}

		// Print success message
		msg := step.SuccessMsg
		if msg == "" {
			msg = step.Title
			// Remove "..." from title if present
			if len(msg) > 3 && msg[len(msg)-3:] == "..." {
				msg = msg[:len(msg)-3]
			}
		}

		if step.Value != "" {
			PrintSuccessWithValue(msg, step.Value)
		} else {
			PrintSuccess(msg)
		}
	}
	return nil
}

// WaitWithSpinner shows a spinner while waiting for a duration
func WaitWithSpinner(title string, duration time.Duration) {
	spinner.New().
		Title("  " + title).
		Action(func() {
			time.Sleep(duration)
		}).
		Run()
}

// SimpleSpinner shows a spinner with a simple title and runs the action
// On success, prints a success message. On error, prints error and returns it.
func SimpleSpinner(title string, fn func() error) error {
	var actionErr error

	err := spinner.New().
		Title("  " + title).
		Action(func() {
			actionErr = fn()
		}).
		Run()

	if err != nil {
		PrintError(err)
		return err
	}
	if actionErr != nil {
		PrintError(actionErr)
		return actionErr
	}

	// Print success - strip trailing "..." from title
	successMsg := title
	if len(successMsg) > 3 && successMsg[len(successMsg)-3:] == "..." {
		successMsg = successMsg[:len(successMsg)-3]
	}
	PrintSuccess(successMsg)
	return nil
}

// SpinnerWithValue runs an action and shows the result value on success
func SpinnerWithValue(title string, fn func() (string, error)) error {
	var value string
	var actionErr error

	err := spinner.New().
		Title("  " + title).
		Action(func() {
			value, actionErr = fn()
		}).
		Run()

	if err != nil {
		PrintError(err)
		return err
	}
	if actionErr != nil {
		PrintError(actionErr)
		return actionErr
	}

	// Print success with value
	successMsg := title
	if len(successMsg) > 3 && successMsg[len(successMsg)-3:] == "..." {
		successMsg = successMsg[:len(successMsg)-3]
	}
	PrintSuccessWithValue(successMsg, value)
	return nil
}

// MountSpinner is specifically for mount operations that need to show status
type MountSpinner struct {
	steps []mountStep
}

type mountStep struct {
	title   string
	success string
	value   string
	fn      func() error
}

// NewMountSpinner creates a new mount spinner sequence
func NewMountSpinner() *MountSpinner {
	return &MountSpinner{}
}

// AddStep adds a step to the mount sequence
func (m *MountSpinner) AddStep(title, successMsg string, fn func() error) *MountSpinner {
	m.steps = append(m.steps, mountStep{
		title:   title,
		success: successMsg,
		fn:      fn,
	})
	return m
}

// AddStepWithValue adds a step that shows a value on completion
func (m *MountSpinner) AddStepWithValue(title, successMsg string, valueFn func() (string, error)) *MountSpinner {
	step := &mountStep{
		title:   title,
		success: successMsg,
	}
	step.fn = func() error {
		val, err := valueFn()
		if err != nil {
			return err
		}
		step.value = val
		return nil
	}
	m.steps = append(m.steps, *step)
	return m
}

// Run executes all steps in sequence
func (m *MountSpinner) Run() error {
	for _, step := range m.steps {
		var actionErr error

		err := spinner.New().
			Title("  " + step.title).
			Action(func() {
				actionErr = step.fn()
			}).
			Run()

		if err != nil {
			return fmt.Errorf("%s: %w", step.title, err)
		}
		if actionErr != nil {
			return actionErr
		}

		// Show success
		msg := step.success
		if msg == "" {
			msg = step.title
			if len(msg) > 3 && msg[len(msg)-3:] == "..." {
				msg = msg[:len(msg)-3]
			}
		}

		if step.value != "" {
			PrintSuccessWithValue(msg, step.value)
		} else {
			PrintSuccess(msg)
		}
	}
	return nil
}
