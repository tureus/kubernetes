/*
Copyright 2016 The Kubernetes Authors All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package etcd

import (
	"time"

	etcdclientv3 "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/storage/storagepb"
	"github.com/golang/glog"
	"golang.org/x/net/context"
	"k8s.io/kubernetes/pkg/runtime"
	"k8s.io/kubernetes/pkg/storage"
	"k8s.io/kubernetes/pkg/watch"
)

// old etcd stuff:
// Etcd watch event actions
const (
	EtcdCreate = "create"
	EtcdGet    = "get"
	EtcdSet    = "set"
	EtcdCAS    = "compareAndSwap"
	EtcdDelete = "delete"
	EtcdExpire = "expire"
)

type etcd3Watcher struct {
	watcher    etcdclientv3.Watcher
	kv         etcdclientv3.KV
	rev        int64
	filter     storage.FilterFunc
	codec      runtime.Codec
	versioner  storage.Versioner
	resultChan chan watch.Event
	stopChan   chan struct{}
}

func newEtcd3Watcher(watcher etcdclientv3.Watcher, kv etcdclientv3.KV, codec runtime.Codec, versioner storage.Versioner, filter storage.FilterFunc) *etcd3Watcher {
	return &etcd3Watcher{
		watcher:    watcher,
		kv:         kv,
		filter:     filter,
		codec:      codec,
		versioner:  versioner,
		resultChan: make(chan watch.Event),
		stopChan:   make(chan struct{}),
	}
}

// ResultChan implements watch.Interface.
func (w *etcd3Watcher) ResultChan() <-chan watch.Event {
	return w.resultChan
}

// Stop implements watch.Interface.
func (w *etcd3Watcher) Stop() {
	// TODO: Stop duplicate to context cancel?
	close(w.stopChan)
}

func (w *etcd3Watcher) startWatching(ctx context.Context, key string, rev int64, recursive bool) {
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)

	go func() {
		select {
		// assuming Stop() is the only way to the finish line
		case <-w.stopChan:
			cancel()
		}
	}()

	w.rev = rev
	var wch etcdclientv3.WatchChan
	opts := []etcdclientv3.OpOption{etcdclientv3.WithRev(rev)}
	if recursive {
		opts = append(opts, etcdclientv3.WithPrefix())
	}
	wch = w.watcher.Watch(ctx, key, opts...)
	for resp := range wch {
		for _, event := range resp.Events {
			w.processEvent(event)
		}
	}
	close(w.resultChan)
}

func (w *etcd3Watcher) processEvent(event *storagepb.Event) {
	if w.rev == event.Kv.ModRevision {
		return
	}
	switch event.Type {
	case storagepb.PUT:
		obj, err := w.decode(event.Kv.Value, event.Kv.ModRevision)
		if err != nil {
			panic(err)
		}
		if !w.filter(obj) {
			return
		}
		eventType := watch.Added
		if event.Kv.CreateRevision != event.Kv.ModRevision {
			eventType = watch.Modified
		}
		e := watch.Event{
			Type:   eventType,
			Object: obj,
		}
		glog.Infof("watch event: type: %s, key: %s, rev: %d", event.Type, event.Kv.Key, event.Kv.ModRevision)
		w.resultChan <- e
	case storagepb.DELETE, storagepb.EXPIRE:
		resp, err := w.kv.Get(context.TODO(), string(event.Kv.Key), etcdclientv3.WithRev(event.Kv.ModRevision-1))
		if err != nil {
			glog.Errorf("Get failed: %v", err)
			return
		}
		obj, err := w.decode(resp.Kvs[0].Value, event.Kv.ModRevision)
		if err != nil {
			panic(err)
		}
		if !w.filter(obj) {
			return
		}
		e := watch.Event{
			Type:   watch.Deleted,
			Object: obj,
		}
		glog.Infof("watch event: type: %s, key: %s, rev: %d", event.Type, event.Kv.Key, event.Kv.ModRevision)
		w.resultChan <- e
	}
}

func (w *etcd3Watcher) decode(body []byte, rev int64) (runtime.Object, error) {
	obj, err := runtime.Decode(w.codec, body)
	if err != nil {
		return nil, err
	}
	if w.versioner != nil {
		todoExpiration := &time.Time{}
		if err := w.versioner.UpdateObject(obj, todoExpiration, uint64(rev)); err != nil {
			return nil, err
		}
	}
	return obj, nil
}
