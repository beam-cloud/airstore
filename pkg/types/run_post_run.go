package types

import "strings"

type RunExecutionPostRun struct {
	WaitingForInput     bool                    `json:"waiting_for_input,omitempty"`
	WakeSignal          *RunExecutionWakeSignal `json:"wake_signal,omitempty"`
	SubtaskRequests     []*SubtaskRequest       `json:"subtask_requests,omitempty"`
	SourceWatchRequests []*SourceWatchRequest   `json:"source_watch_requests,omitempty"`
}

func NormalizeRunExecutionPostRun(postRun *RunExecutionPostRun) *RunExecutionPostRun {
	if postRun == nil {
		return nil
	}

	normalized := *postRun
	normalized.WakeSignal = NormalizeRunExecutionWakeSignal(normalized.WakeSignal)
	normalized.SubtaskRequests = NormalizeSubtaskRequests(normalized.SubtaskRequests)
	normalized.SourceWatchRequests = NormalizeSourceWatchRequestList(normalized.SourceWatchRequests)

	if !normalized.WaitingForInput &&
		normalized.WakeSignal == nil &&
		len(normalized.SubtaskRequests) == 0 &&
		len(normalized.SourceWatchRequests) == 0 {
		return nil
	}

	return &normalized
}

func NormalizeRunExecutionWakeSignal(signal *RunExecutionWakeSignal) *RunExecutionWakeSignal {
	if signal == nil {
		return nil
	}

	normalized := *signal
	normalized.Reason = strings.TrimSpace(normalized.Reason)
	normalized.FollowUpPrompt = strings.TrimSpace(normalized.FollowUpPrompt)
	if normalized.DelayMinutes < 0 {
		normalized.DelayMinutes = 0
	}

	agenda := make([]*TaskWakeAgendaItem, 0, len(normalized.WakeAgenda))
	for idx, item := range normalized.WakeAgenda {
		if item == nil {
			continue
		}
		agendaItem := *item
		agendaItem.Type = strings.TrimSpace(agendaItem.Type)
		agendaItem.Title = strings.TrimSpace(agendaItem.Title)
		agendaItem.Reason = strings.TrimSpace(agendaItem.Reason)
		if agendaItem.Title == "" {
			agendaItem.Title = agendaItem.Reason
		}
		if agendaItem.Title == "" && agendaItem.Reason == "" {
			continue
		}
		if agendaItem.Seq <= 0 {
			agendaItem.Seq = idx + 1
		}
		agenda = append(agenda, &agendaItem)
	}
	normalized.WakeAgenda = agenda

	if normalized.DelayMinutes <= 0 &&
		normalized.Reason == "" &&
		normalized.FollowUpPrompt == "" &&
		len(normalized.WakeAgenda) == 0 {
		return nil
	}

	return &normalized
}

func NormalizeSubtaskRequests(requests []*SubtaskRequest) []*SubtaskRequest {
	if len(requests) == 0 {
		return nil
	}

	normalized := make([]*SubtaskRequest, 0, len(requests))
	for _, req := range requests {
		if req == nil {
			continue
		}
		item := *req
		item.SourceOutputID = strings.TrimSpace(item.SourceOutputID)
		item.EntityLabel = strings.TrimSpace(item.EntityLabel)
		item.Prompt = strings.TrimSpace(item.Prompt)
		if item.WakeDelayMinutes < 0 {
			item.WakeDelayMinutes = 0
		}
		if item.Prompt == "" {
			continue
		}
		normalized = append(normalized, &item)
	}

	if len(normalized) == 0 {
		return nil
	}
	return normalized
}

func NormalizeSourceWatchRequestList(requests []*SourceWatchRequest) []*SourceWatchRequest {
	if len(requests) == 0 {
		return nil
	}

	normalized := make([]*SourceWatchRequest, 0, len(requests))
	for _, req := range requests {
		if item := NormalizeSourceWatchRequest(req); item != nil {
			normalized = append(normalized, item)
		}
	}
	if len(normalized) == 0 {
		return nil
	}
	return normalized
}

func (r *RunExecutionResult) SetPostRun(postRun *RunExecutionPostRun) {
	if r == nil {
		return
	}
	normalized := NormalizeRunExecutionPostRun(postRun)
	r.PostRun = normalized
	if normalized == nil {
		r.WaitingForInput = false
		r.WakeSignal = nil
		r.SubtaskRequests = nil
		r.SourceWatchRequests = nil
		return
	}

	r.WaitingForInput = normalized.WaitingForInput
	r.WakeSignal = normalized.WakeSignal
	r.SubtaskRequests = normalized.SubtaskRequests
	r.SourceWatchRequests = normalized.SourceWatchRequests
}

func (r *RunExecutionResult) NormalizedPostRun() *RunExecutionPostRun {
	if r == nil {
		return nil
	}
	if normalized := NormalizeRunExecutionPostRun(r.PostRun); normalized != nil {
		return normalized
	}
	return NormalizeRunExecutionPostRun(&RunExecutionPostRun{
		WaitingForInput:     r.WaitingForInput,
		WakeSignal:          r.WakeSignal,
		SubtaskRequests:     r.SubtaskRequests,
		SourceWatchRequests: r.SourceWatchRequests,
	})
}
