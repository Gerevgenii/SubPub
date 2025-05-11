package cmd

import (
	"context"
	"errors"
	"sync"
)

func NewSubPub() SubPub {
	return &subPub{
		subscribers: make(map[string]map[*subscriber]struct{}),
	}
}

type subPub struct {
	mutex       sync.RWMutex
	subscribers map[string]map[*subscriber]struct{}
	isClosed    bool
	isAborted   sync.Once
	wg          sync.WaitGroup
}

type subscriber struct {
	message   MessageHandler
	msgCh     chan interface{}
	isAborted sync.Once
	stopChan  chan struct{}
}

type subscription struct {
	subject    string
	sp         *subPub
	subscriber *subscriber
}

func (s *subPub) Subscribe(subject string, msg MessageHandler) Subscription {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.isClosed {
		return nil
	}

	sub := &subscriber{
		message:  msg,
		msgCh:    make(chan interface{}, 64), // Данный буфер не вызывает deadlock. Он нужен для медленных пользователей
		stopChan: make(chan struct{}),
	}

	if s.subscribers[subject] == nil {
		s.subscribers[subject] = make(map[*subscriber]struct{})
	}
	s.subscribers[subject][sub] = struct{}{}

	s.wg.Add(1)
	go s.runSubscriber(sub)

	return &subscription{
		subject:    subject,
		sp:         s,
		subscriber: sub,
	}
}

func (s *subPub) runSubscriber(sub *subscriber) {
	defer s.wg.Done()
	for {
		select {
		case msg, ok := <-sub.msgCh:
			if !ok {
				return
			}
			sub.message(msg)
		case <-sub.stopChan:
			return
		}
	}
}

func (s *subPub) Publish(subject string, msg interface{}) error {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	if s.isClosed {
		return errors.New("is already closed")
	}

	for sub := range s.subscribers[subject] {
		select {
		case sub.msgCh <- msg:
		default:
			sub.msgCh <- msg
		}
	}
	return nil
}

func (s *subPub) Close(ctx context.Context) error {
	s.isAborted.Do(func() {
		s.mutex.Lock()
		s.isClosed = true
		s.mutex.Unlock()
	})

	_, hasDeadline := ctx.Deadline()
	if !hasDeadline {
		s.mutex.Lock()
		var all []*subscriber
		for _, subs := range s.subscribers {
			for sub := range subs {
				all = append(all, sub)
			}
		}
		s.subscribers = nil
		s.mutex.Unlock()

		for _, sub := range all {
			sub.isAborted.Do(func() {
				close(sub.stopChan)
				close(sub.msgCh)
			})
		}
		s.wg.Wait()
		return nil
	}

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *subscription) Unsubscribe() {
	s.sp.unsubscribe(s.subject, s.subscriber)
}

func (s *subPub) unsubscribe(subject string, subscriber *subscriber) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	topicSubscribers, ok := s.subscribers[subject]
	if !ok {
		return
	}
	if _, exists := topicSubscribers[subscriber]; exists {
		delete(topicSubscribers, subscriber)
		subscriber.isAborted.Do(func() {
			close(subscriber.stopChan)
			close(subscriber.msgCh)
		})
	}
	if len(topicSubscribers) == 0 {
		delete(s.subscribers, subject)
	}
}
