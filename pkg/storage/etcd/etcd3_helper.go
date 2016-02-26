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
	"errors"
	"fmt"
	"path"
	"reflect"
	"strings"
	"time"

	etcdclientv3 "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/storage/storagepb"
	"github.com/golang/glog"
	"golang.org/x/net/context"
	"k8s.io/kubernetes/pkg/api/meta"
	"k8s.io/kubernetes/pkg/conversion"
	"k8s.io/kubernetes/pkg/runtime"
	"k8s.io/kubernetes/pkg/storage"
	etcdutil "k8s.io/kubernetes/pkg/storage/etcd/util"
	"k8s.io/kubernetes/pkg/watch"
)

// storage.Config object for etcd.
type EtcdConfig struct {
	ServerList []string
	Codec      runtime.Codec
	Prefix     string
	Quorum     bool
}

// implements storage.Config
func (c *EtcdConfig) GetType() string {
	return "etcd"
}

func (c *EtcdConfig) NewStorage() (storage.Interface, error) {
	//glog.Infof("eeeeetcd: NewStorage")
	//debug.PrintStack()
	cfg := &etcdclientv3.Config{
		Endpoints: c.ServerList,
	}
	client, err := etcdclientv3.New(*cfg)
	if err != nil {
		return nil, err
	}
	return newEtcd3Helper(client, c.Codec, c.Prefix), nil
}

func NewEtcdStorage(client *etcdclientv3.Client, codec runtime.Codec, prefix string, quorum bool) storage.Interface {
	//glog.Infof("eeeeetcd: NewEtcdStorage")
	//debug.PrintStack()
	return newEtcd3Helper(client, codec, prefix)
}

// lease manager design:
// Lease manager manages many keys and lease clients.
// In local, we can set the expiration time to do lazy expiration.
// For remote, we can attach each key to one lease as the first step.

type etcd3Helper struct {
	// etcd interfaces
	client      *etcdclientv3.Client
	kvClient    etcdclientv3.KV
	leaseClient etcdclientv3.Lease
	watcher     etcdclientv3.Watcher

	codec runtime.Codec
	// optional, has to be set to perform any atomic operations
	versioner storage.Versioner
	// prefix for all etcd keys
	pathPrefix string
}

func newEtcd3Helper(c *etcdclientv3.Client, codec runtime.Codec, prefix string) *etcd3Helper {
	return &etcd3Helper{
		client:      c,
		kvClient:    etcdclientv3.NewKV(c),
		leaseClient: etcdclientv3.NewLease(c),
		watcher:     etcdclientv3.NewWatcher(c),
		versioner:   APIObjectVersioner{},
		codec:       codec,
		pathPrefix:  prefix,
	}
}

func (h *etcd3Helper) Create(ctx context.Context, key string, obj, out runtime.Object, ttl uint64) error {
	if ctx == nil {
		glog.Errorf("Context is nil")
		ctx = context.TODO()
	}
	key = h.prefixEtcdKey(key)
	data, err := runtime.Encode(h.codec, obj)
	if err != nil {
		return err
	}
	if h.versioner != nil {
		if version, err := h.versioner.ObjectResourceVersion(obj); err == nil && version != 0 {
			return errors.New("resourceVersion may not be set on objects to be created")
		}
	}

	resp, err := h.kvClient.Txn(ctx).If(
		etcdclientv3.Compare(etcdclientv3.Version(key), "<", 1),
	).Then(
		etcdclientv3.OpPut(key, string(data)),
	).Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return etcdutil.ErrAlreadyExist
	}
	putResp := resp.Responses[0].GetResponsePut()

	if out != nil {
		if _, err := conversion.EnforcePtr(out); err != nil {
			panic("unable to convert output object to pointer")
		}
		err = h.decode(data, out, putResp.Header.Revision)
		if err != nil {
			return err
		}
	}
	return nil
}

func (h *etcd3Helper) Set(ctx context.Context, key string, obj, out runtime.Object, ttl uint64) error {
	if ctx == nil {
		glog.Errorf("Context is nil")
		ctx = context.TODO()
	}

	version := uint64(0)
	if h.versioner != nil {
		var err error
		version, err = h.versioner.ObjectResourceVersion(obj)
		if err != nil {
			return err
		}
		if version != 0 {
			// We cannot store object with resourceVersion in etcd. We need to reset it.
			err := h.versioner.UpdateObject(obj, nil, 0)
			if err != nil {
				return fmt.Errorf("UpdateObject failed: %v", err)
			}
		}
	}

	// TODO: If versioner is nil, then we may end up with having ResourceVersion set
	// in the object and this will be incorrect ResourceVersion. We should fix it by
	// requiring "versioner != nil" at the constructor level for 1.3 milestone.

	data, err := runtime.Encode(h.codec, obj)
	if err != nil {
		return err
	}
	key = h.prefixEtcdKey(key)

	txnResp, err := h.kvClient.Txn(ctx).If(
		etcdclientv3.Compare(etcdclientv3.Version(key), "=", int64(version)),
	).Then(
		etcdclientv3.OpPut(key, string(data)),
	).Commit()
	if err != nil {
		return err
	}
	if !txnResp.Succeeded {
		return fmt.Errorf("Set failed!")
	}
	putResp := txnResp.Responses[0].GetResponsePut()

	if out != nil {
		if _, err := conversion.EnforcePtr(out); err != nil {
			panic("unable to convert output object to pointer")
		}
		err = h.decode(data, out, putResp.Header.Revision)
		if err != nil {
			return err
		}
	}

	return nil
}

// Implements storage.Interface.
func (h *etcd3Helper) Delete(ctx context.Context, key string, out runtime.Object) error {
	if ctx == nil {
		glog.Errorf("Context is nil")
		ctx = context.TODO()
	}

	key = h.prefixEtcdKey(key)
	if _, err := conversion.EnforcePtr(out); err != nil {
		panic("unable to convert output object to pointer")
	}

	txnResp, err := h.kvClient.Txn(ctx).If().Then(
		etcdclientv3.OpGet(key),
		etcdclientv3.OpDelete(key),
	).Commit()
	if err != nil {
		return err
	}
	getResp := txnResp.Responses[0].GetResponseRange()
	if len(getResp.Kvs) > 0 {
		err = h.decode(getResp.Kvs[0].Value, out, getResp.Kvs[0].ModRevision)
		if err != nil {
			return err
		}
		return nil
	} else {
		return etcdutil.ErrNotFound
	}
}

func (h *etcd3Helper) Get(ctx context.Context, key string, objPtr runtime.Object, ignoreNotFound bool) error {
	if ctx == nil {
		glog.Errorf("Context is nil")
		ctx = context.TODO()
	}
	key = h.prefixEtcdKey(key)

	resp, err := h.kvClient.Get(ctx, key)
	if err != nil {
		return err
	}

	// if not found:
	//   if ignoreNotFound: return zero
	//   else: return error
	if len(resp.Kvs) == 0 {
		if ignoreNotFound {
			return setZero(objPtr)
		} else {
			return etcdutil.ErrNotFound
		}
	}

	err = h.decode(resp.Kvs[0].Value, objPtr, resp.Kvs[0].ModRevision)
	if err != nil {
		return err
	}
	return nil
}

func (h *etcd3Helper) GetToList(ctx context.Context, key string, filter storage.FilterFunc, listObj runtime.Object) error {
	if ctx == nil {
		glog.Errorf("Context is nil")
		ctx = context.TODO()
	}
	listPtr, err := meta.GetItemsPtr(listObj)
	if err != nil {
		return err
	}
	key = h.prefixEtcdKey(key)
	resp, err := h.kvClient.Get(ctx, key)
	if err != nil {
		return err
	}

	if len(resp.Kvs) < 1 {
		return etcdutil.ErrNotFound
	}
	kvs := append([]*storagepb.KeyValue(nil), resp.Kvs[0])
	if err := h.decodeNodeList(kvs, filter, listPtr); err != nil {
		return err
	}

	if h.versioner != nil {
		if err := h.versioner.UpdateList(listObj, uint64(resp.Header.Revision)); err != nil {
			return err
		}
	}
	return nil
}

// Implements storage.Interface.
func (h *etcd3Helper) List(ctx context.Context, key, resourceVersion string, filter storage.FilterFunc, listObj runtime.Object) error {
	if ctx == nil {
		glog.Errorf("Context is nil")
		ctx = context.TODO()
	}
	key = h.prefixEtcdKey(key)
	if key[len(key)-1:] != "/" {
		key += "/"
	}
	resp, err := h.kvClient.Get(ctx, key, etcdclientv3.WithPrefix())
	if err != nil {
		return err
	}

	listPtr, err := meta.GetItemsPtr(listObj)
	if err != nil {
		return err
	}
	if err := h.decodeNodeList(resp.Kvs, filter, listPtr); err != nil {
		return err
	}

	if h.versioner != nil {
		if err := h.versioner.UpdateList(listObj, uint64(resp.Header.Revision)); err != nil {
			return err
		}
	}
	return nil
}

// Implements storage.Interface.
func (h *etcd3Helper) Watch(ctx context.Context, key string, resourceVersion string, filter storage.FilterFunc) (watch.Interface, error) {
	if ctx == nil {
		glog.Errorf("Context is nil")
		ctx = context.TODO()
	}
	watchRV, err := storage.ParseWatchResourceVersion(resourceVersion)
	if err != nil {
		return nil, err
	}
	key = h.prefixEtcdKey(key)

	w := newEtcd3Watcher(h.watcher, h.kvClient, h.codec, h.versioner, filter)

	rev := int64(watchRV)
	// We need to get the initial state if watch from 0
	if rev == 0 {
		resp, err := w.kv.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		if len(resp.Kvs) > 0 {
			rev = resp.Kvs[0].ModRevision
			obj, err := w.decode(resp.Kvs[0].Value, rev)
			if err != nil {
				panic(err)
			}
			if w.filter(obj) {
				eventType := watch.Added
				if resp.Kvs[0].CreateRevision != resp.Kvs[0].ModRevision {
					eventType = watch.Modified
				}
				e := watch.Event{
					Type:   eventType,
					Object: obj,
				}
				glog.Infof("watch event: type: %s, key: %s, rev: %d", eventType, key, rev)
				w.resultChan <- e
			}
		}
	}

	go w.startWatching(ctx, key, rev, false)
	return w, nil
}

// Implements storage.Interface.
func (h *etcd3Helper) WatchList(ctx context.Context, key string, resourceVersion string, filter storage.FilterFunc) (watch.Interface, error) {
	if ctx == nil {
		glog.Errorf("Context is nil")
		ctx = context.TODO()
	}
	watchRV, err := storage.ParseWatchResourceVersion(resourceVersion)
	if err != nil {
		return nil, err
	}
	key = h.prefixEtcdKey(key)
	if key[len(key)-1:] != "/" {
		key += "/"
	}

	w := newEtcd3Watcher(h.watcher, h.kvClient, h.codec, h.versioner, filter)

	rev := int64(watchRV)
	// We need to get the initial state if watch from 0
	if rev == 0 {
		resp, err := w.kv.Get(ctx, key, etcdclientv3.WithPrefix())
		if err != nil {
			return nil, err
		}
		rev = resp.Header.Revision

		for _, kv := range resp.Kvs {
			kvRev := kv.ModRevision
			obj, err := w.decode(kv.Value, kvRev)
			if err != nil {
				panic(err)
			}
			if w.filter(obj) {
				eventType := watch.Added
				if kv.CreateRevision != kv.ModRevision {
					eventType = watch.Modified
				}
				e := watch.Event{
					Type:   eventType,
					Object: obj,
				}
				glog.Infof("watch event: type: %s, key: %s, rev: %d", eventType, key, kvRev)
				w.resultChan <- e
			}
		}
	}

	go w.startWatching(ctx, key, rev, true)
	return w, nil
}

// Implements storage.Interface.
func (h *etcd3Helper) GuaranteedUpdate(ctx context.Context, key string, ptrToType runtime.Object, ignoreNotFound bool, tryUpdate storage.UpdateFunc) error {
	if ctx == nil {
		glog.Errorf("Context is nil")
		ctx = context.TODO()
	}
	v, err := conversion.EnforcePtr(ptrToType)
	if err != nil {
		panic("need ptr to type")
	}
	key = h.prefixEtcdKey(key)
	// keep retrying if there is an index conflict.
	// get -> user func -> set
	for {
		// get initial obj and update meta
		resp, err := h.kvClient.Get(ctx, key)
		if err != nil {
			return err
		}

		obj := reflect.New(v.Type()).Interface().(runtime.Object)
		meta := &storage.ResponseMeta{}
		rev := int64(0)
		var oriBody string
		if len(resp.Kvs) > 0 {
			rev = resp.Kvs[0].ModRevision
			meta.ResourceVersion = uint64(resp.Kvs[0].ModRevision)
			err = h.decode(resp.Kvs[0].Value, obj, resp.Kvs[0].ModRevision)
			if err != nil {
				return err
			}
			oriBody = string(resp.Kvs[0].Value)
		} else {
			if !ignoreNotFound {
				return etcdutil.ErrNotFound
			}
			setZero(obj)
		}

		// Get the object to be written by calling tryUpdate.
		ret, _, err := tryUpdate(obj, *meta)
		if err != nil {
			return err
		}

		if h.versioner != nil {
			var err error
			ver, err := h.versioner.ObjectResourceVersion(ret)
			if err != nil {
				return err
			}
			if ver != 0 {
				// We cannot store object with resourceVersion in etcd. We need to reset it.
				err := h.versioner.UpdateObject(ret, nil, 0)
				if err != nil {
					return fmt.Errorf("UpdateObject failed: %v", err)
				}
			}
		}
		data, err := runtime.Encode(h.codec, ret)
		if err != nil {
			return err
		}

		if string(data) == oriBody {
			return h.decode(data, ptrToType, rev)
		}

		txnResp, err := h.kvClient.Txn(ctx).If(
			etcdclientv3.Compare(etcdclientv3.ModifiedRevision(key), "=", rev),
		).Then(
			etcdclientv3.OpPut(key, string(data)),
		).Commit()
		if err != nil {
			return err
		}
		if !txnResp.Succeeded {
			continue
		}
		putResp := txnResp.Responses[0].GetResponsePut()
		return h.decode(data, ptrToType, putResp.Header.Revision)
	}
	return nil
}

// Implements storage.Interface.
func (h *etcd3Helper) Backends(ctx context.Context) []string {
	resp, err := etcdclientv3.NewCluster(h.client).MemberList(ctx)
	if err != nil {
		glog.Errorf("Error obtaining etcd members list: %q", err)
		return nil
	}
	var mlist []string
	for _, member := range resp.Members {
		mlist = append(mlist, member.ClientURLs...)
	}
	return mlist
}

// Codec provides access to the underlying codec being used by the implementation.
func (h *etcd3Helper) Codec() runtime.Codec {
	return h.codec
}

// Implements storage.Interface.
func (h *etcd3Helper) Versioner() storage.Versioner {
	return h.versioner
}

func (h *etcd3Helper) decode(body []byte, objPtr runtime.Object, rev int64) error {
	_, _, err := h.codec.Decode(body, nil, objPtr)
	if h.versioner != nil {
		h.versioner.UpdateObject(objPtr, &time.Time{}, uint64(rev))
	}
	return err
}

func (h *etcd3Helper) decodeNodeList(kvs []*storagepb.KeyValue, filter storage.FilterFunc, slicePtr interface{}) error {
	v, err := conversion.EnforcePtr(slicePtr)
	if err != nil || v.Kind() != reflect.Slice {
		panic("need ptr to slice")
	}
	for _, kv := range kvs {
		obj, _, err := h.codec.Decode([]byte(kv.Value), nil, reflect.New(v.Type().Elem()).Interface().(runtime.Object))
		if err != nil {
			return err
		}
		if h.versioner != nil {
			todoExpiration := &time.Time{}
			// being unable to set the version does not prevent the object from being extracted
			h.versioner.UpdateObject(obj, todoExpiration, uint64(kv.ModRevision))
		}
		if filter(obj) {
			v.Set(reflect.Append(v, reflect.ValueOf(obj).Elem()))
		}
	}
	return nil
}

func (h *etcd3Helper) prefixEtcdKey(key string) string {
	if strings.HasPrefix(key, h.pathPrefix) {
		return key
	}
	return path.Join(h.pathPrefix, key)
}

func setZero(objPtr runtime.Object) error {
	v, err := conversion.EnforcePtr(objPtr)
	if err != nil {
		return err
	}
	v.Set(reflect.Zero(v.Type()))
	return nil
}
