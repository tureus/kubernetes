/*
Copyright 2014 The Kubernetes Authors All rights reserved.

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
	"log"
	"reflect"
	goruntime "runtime"
	"testing"
	"time"

	etcdclientv3 "github.com/coreos/etcd/clientv3"
	etcdintegration "github.com/coreos/etcd/integration"
	"golang.org/x/net/context"
	"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/api/testapi"
	apitesting "k8s.io/kubernetes/pkg/api/testing"
	"k8s.io/kubernetes/pkg/conversion"
	"k8s.io/kubernetes/pkg/runtime"
	"k8s.io/kubernetes/pkg/runtime/serializer"
	"k8s.io/kubernetes/pkg/storage"
	"k8s.io/kubernetes/pkg/storage/etcd/etcdtest"
	storagetesting "k8s.io/kubernetes/pkg/storage/testing"
	"k8s.io/kubernetes/pkg/watch"
)

func TestETCD3(t *testing.T) {
	clus := etcdintegration.NewClusterV3(t, &etcdintegration.ClusterConfig{Size: 1})
	defer clus.Terminate(t)

	testSet(t, "testSet", clus)
	testList(t, "testList", clus)
	testCreate(t, "testCreate", clus)
	testDelete(t, "testDelete", clus)
	testWatch(t, "testWatch", clus)
	testWatchListFromZeroIndex(t, "testWatchListFromZeroIndex", clus)
	testWatchListIgnoresRootKey(t, "testWatchListIgnoresRootKey", clus)
	testGuaranteedUpdate(t, "testGuaranteedUpdate", clus)
	testGetToList(t, "testGetToList", clus)
}

func testCreate(t *testing.T, testcaseName string, clus *etcdintegration.ClusterV3) {
	log.Printf("=== running %s ===", testcaseName)

	etcdClient := clus.RandClient()
	defer etcdclientv3.NewKV(etcdClient).Delete(context.TODO(), "\x00", etcdclientv3.WithRange("\xff"))
	obj := &api.Pod{ObjectMeta: api.ObjectMeta{Name: "foo"}}
	helper := newEtcd3Helper(etcdClient, testapi.Default.Codec(), etcdtest.PathPrefix())
	returnedObj := &api.Pod{}
	err := helper.Create(context.TODO(), "/some/key", obj, returnedObj, 5)
	if err != nil {
		t.Errorf("Unexpected error %#v", err)
		return
	}
	err = helper.Get(context.TODO(), "/some/key", returnedObj, false)
	if err != nil {
		t.Errorf("Unexpected error %#v", err)
		return
	}
	_, err = runtime.Encode(testapi.Default.Codec(), returnedObj)
	if err != nil {
		t.Errorf("Unexpected error %#v", err)
		return
	}
	if obj.Name != returnedObj.Name {
		t.Errorf("Wanted %v, got %v", obj.Name, returnedObj.Name)
		return
	}
	log.Printf("=== pass %s ===", testcaseName)
}

func testDelete(t *testing.T, testcaseName string, clus *etcdintegration.ClusterV3) {
	log.Printf("=== running %s ===", testcaseName)

	etcdClient := clus.RandClient()
	defer etcdclientv3.NewKV(etcdClient).Delete(context.TODO(), "\x00", etcdclientv3.WithRange("\xff"))
	obj := &api.Pod{ObjectMeta: api.ObjectMeta{Name: "foo"}}
	helper := newEtcd3Helper(etcdClient, testapi.Default.Codec(), etcdtest.PathPrefix())
	err := helper.Create(context.TODO(), "/some/key", obj, nil, 5)
	if err != nil {
		t.Errorf("Unexpected error %#v", err)
		return
	}
	returnedObj := &api.Pod{}
	err = helper.Delete(context.TODO(), "/some/key", returnedObj)
	if err != nil {
		t.Errorf("Unexpected error %#v", err)
		return
	}
	if obj.Name != returnedObj.Name {
		t.Errorf("Wanted %v, got %v", obj.Name, returnedObj.Name)
		return
	}
	returnedObj = &api.Pod{}
	err = helper.Get(context.TODO(), "/some/key", returnedObj, true)
	if err != nil {
		t.Errorf("Unexpected error %#v", err)
		return
	}
	if returnedObj.Name != "" {
		t.Errorf("Wanted empty obj, got %#v", returnedObj)
		return
	}
	log.Printf("=== pass %s ===", testcaseName)
}

func testList(t *testing.T, testcaseName string, clus *etcdintegration.ClusterV3) {
	log.Printf("=== running %s ===", testcaseName)

	etcdClient := clus.RandClient()
	defer etcdclientv3.NewKV(etcdClient).Delete(context.TODO(), "\x00", etcdclientv3.WithRange("\xff"))
	key := etcdtest.AddPrefix("/some/key")
	helper := newEtcd3Helper(etcdClient, testapi.Default.Codec(), key)

	pods := api.PodList{
		Items: []api.Pod{
			{
				ObjectMeta: api.ObjectMeta{Name: "bar"},
				Spec:       apitesting.DeepEqualSafePodSpec(),
			},
			{
				ObjectMeta: api.ObjectMeta{Name: "baz"},
				Spec:       apitesting.DeepEqualSafePodSpec(),
			},
			{
				ObjectMeta: api.ObjectMeta{Name: "foo"},
				Spec:       apitesting.DeepEqualSafePodSpec(),
			},
		},
	}

	err := createPods(t, helper, &pods)
	if err != nil {
		t.Errorf("createPodList failed: %v", err)
		return
	}
	var got api.PodList
	// TODO: a sorted filter function could be applied such implied
	// ordering on the returned list doesn't matter.
	err = helper.List(context.TODO(), key, "", storage.Everything, &got)
	if err != nil {
		t.Errorf("list failed: %v", err)
		return
	}
	if e, a := pods.Items, got.Items; !reflect.DeepEqual(e, a) {
		t.Errorf("want=%#v\ngot %#v", e, a)
		return
	}
	log.Printf("=== pass %s ===", testcaseName)
}

func testSet(t *testing.T, testcaseName string, clus *etcdintegration.ClusterV3) {
	log.Printf("=== running %s ===", testcaseName)

	etcdClient := clus.RandClient()
	defer etcdclientv3.NewKV(etcdClient).Delete(context.TODO(), "\x00", etcdclientv3.WithRange("\xff"))
	helper := newEtcd3Helper(etcdClient, testapi.Default.Codec(), etcdtest.PathPrefix())

	key := "/some/key"
	obj := &api.Pod{ObjectMeta: api.ObjectMeta{Name: "foo"}}
	returnedObj := &api.Pod{}
	err := helper.Set(context.Background(), key, obj, returnedObj, 0)
	if err != nil {
		t.Errorf("Set failed: %#v", err)
		return
	}

	if obj.ObjectMeta.Name == returnedObj.ObjectMeta.Name {
		// Set worked, now override the values.
		obj = returnedObj
	}

	err = helper.Get(context.Background(), key, returnedObj, false)
	if err != nil {
		t.Errorf("Get failed: %#v", err)
		return
	}
	if !reflect.DeepEqual(obj, returnedObj) {
		t.Errorf("want=%#v\ngot=%#v", obj, returnedObj)
		return
	}
	log.Printf("=== pass %s ===", testcaseName)
}

func testWatch(t *testing.T, testcaseName string, clus *etcdintegration.ClusterV3) {
	log.Printf("=== running %s ===", testcaseName)

	etcdClient := clus.RandClient()
	defer etcdclientv3.NewKV(etcdClient).Delete(context.TODO(), "\x00", etcdclientv3.WithRange("\xff"))
	key := "/some/key"
	h := newEtcd3Helper(etcdClient, testapi.Default.Codec(), etcdtest.PathPrefix())

	watching, err := h.Watch(context.TODO(), key, "0", storage.Everything)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// watching is explicitly closed below.

	pod := &api.Pod{ObjectMeta: api.ObjectMeta{Name: "foo"}}
	returnObj := &api.Pod{}
	err = h.Set(context.TODO(), key, pod, returnObj, 0)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
		return
	}

	event := <-watching.ResultChan()
	if e, a := watch.Added, event.Type; e != a {
		t.Errorf("Expected %v, got %v", e, a)
		return
	}
	if e, a := pod, event.Object; !api.Semantic.DeepDerivative(e, a) {
		t.Errorf("Expected %v, got %v", e, a)
		return
	}

	watching.Stop()

	event, open := <-watching.ResultChan()
	if open {
		t.Errorf("Unexpected event from stopped watcher: %#v", event)
		return
	}
	log.Printf("=== pass %s ===", testcaseName)
}

func testWatchListFromZeroIndex(t *testing.T, testcaseName string, clus *etcdintegration.ClusterV3) {
	log.Printf("=== running %s ===", testcaseName)

	etcdClient := clus.RandClient()
	defer etcdclientv3.NewKV(etcdClient).Delete(context.TODO(), "\x00", etcdclientv3.WithRange("\xff"))
	key := etcdtest.AddPrefix("/some/key")
	prefix := key
	h := newEtcd3Helper(etcdClient, testapi.Default.Codec(), prefix)

	watching, err := h.WatchList(context.TODO(), key, "0", storage.Everything)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
		return
	}
	defer watching.Stop()

	// creates key/foo which should trigger the WatchList for "key"
	pod := &api.Pod{ObjectMeta: api.ObjectMeta{Name: "foo"}}
	err = h.Create(context.TODO(), pod.Name, pod, nil, 0)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
		return
	}

	event := <-watching.ResultChan()
	if event.Type != watch.Added {
		t.Errorf("Unexpected event %#v", event)
		return
	}

	if e, a := pod, event.Object; !api.Semantic.DeepDerivative(e, a) {
		t.Errorf("expected %v, got %v", e, a)
		return
	}

	log.Printf("=== pass %s ===", testcaseName)
}

func testWatchListIgnoresRootKey(t *testing.T, testcaseName string, clus *etcdintegration.ClusterV3) {
	log.Printf("=== running %s ===", testcaseName)

	etcdClient := clus.RandClient()
	defer etcdclientv3.NewKV(etcdClient).Delete(context.TODO(), "\x00", etcdclientv3.WithRange("\xff"))
	key := etcdtest.AddPrefix("/some/key")
	prefix := key
	h := newEtcd3Helper(etcdClient, testapi.Default.Codec(), prefix)
	pod := &api.Pod{ObjectMeta: api.ObjectMeta{Name: "foo"}}

	watching, err := h.WatchList(context.TODO(), key, "0", storage.Everything)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
		return
	}
	defer watching.Stop()

	// creates key/foo which should trigger the WatchList for "key"
	err = h.Create(context.TODO(), key, pod, pod, 0)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
		return
	}

	// force context switch to ensure watches would catch and notify.
	goruntime.Gosched()

	select {
	case event := <-watching.ResultChan():
		t.Fatalf("Unexpected event: %#v", event)
		return
	default:
		// fall through, expected behavior
	}

	log.Printf("=== pass %s ===", testcaseName)
}

func testGuaranteedUpdate(t *testing.T, testcaseName string, clus *etcdintegration.ClusterV3) {
	log.Printf("=== running %s ===", testcaseName)
	_, codec := testScheme(t)

	key := etcdtest.AddPrefix("/some/key")
	prefix := key
	etcdClient := clus.RandClient()
	defer etcdclientv3.NewKV(etcdClient).Delete(context.TODO(), "\x00", etcdclientv3.WithRange("\xff"))
	helper := newEtcd3Helper(etcdClient, codec, prefix)

	obj := &storagetesting.TestResource{ObjectMeta: api.ObjectMeta{Name: "foo"}, Value: 1}
	err := helper.GuaranteedUpdate(context.TODO(), key, &storagetesting.TestResource{}, true, storage.SimpleUpdate(func(in runtime.Object) (runtime.Object, error) {
		return obj, nil
	}))
	if err != nil {
		t.Errorf("Unexpected error %#v", err)
		return
	}

	// Update an existing node.
	objUpdate := &storagetesting.TestResource{ObjectMeta: api.ObjectMeta{Name: "foo"}, Value: 2}
	err = helper.GuaranteedUpdate(context.TODO(), key, &storagetesting.TestResource{}, true, storage.SimpleUpdate(func(in runtime.Object) (runtime.Object, error) {
		if in.(*storagetesting.TestResource).Value != 1 {
			t.Errorf("Callback input was not current set value")
		}

		return objUpdate, nil
	}))
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	objCheck := &storagetesting.TestResource{}
	err = helper.Get(context.TODO(), key, objCheck, false)
	if err != nil {
		t.Errorf("Unexpected error %#v", err)
		return
	}
	if objCheck.Value != 2 {
		t.Errorf("Value should have been 2 but got %v", objCheck.Value)
		return
	}

	log.Printf("=== pass %s ===", testcaseName)
}

func testGetToList(t *testing.T, testcaseName string, clus *etcdintegration.ClusterV3) {
	log.Printf("=== running %s ===", testcaseName)

	etcdClient := clus.RandClient()
	defer etcdclientv3.NewKV(etcdClient).Delete(context.TODO(), "\x00", etcdclientv3.WithRange("\xff"))
	helper := newEtcd3Helper(etcdClient, testapi.Default.Codec(), etcdtest.PathPrefix())

	key := "/some/key"
	obj := &api.Pod{ObjectMeta: api.ObjectMeta{Name: "foo"}}
	returnedObj := &api.Pod{}
	err := helper.Set(context.Background(), key, obj, returnedObj, 0)
	if err != nil {
		t.Errorf("Set failed: %#v", err)
		return
	}

	if obj.ObjectMeta.Name == returnedObj.ObjectMeta.Name {
		// Set worked, now override the values.
		obj = returnedObj
	}
	pods := &api.PodList{}
	err = helper.GetToList(context.Background(), key, storage.Everything, pods)
	if err != nil {
		t.Errorf("GetToList failed: %#v", err)
		return
	}
	if len(pods.Items) < 1 {
		t.Errorf("No pod was returned!")
		return
	}
	returnedObj = &pods.Items[0]
	if !reflect.DeepEqual(obj, returnedObj) {
		t.Errorf("want=%#v\ngot=%#v", obj, returnedObj)
		return
	}
	log.Printf("=== pass %s ===", testcaseName)
}

func createObjWithEtcd3(t *testing.T, helper *etcd3Helper, name string, obj, out runtime.Object, ttl uint64) error {
	err := helper.Set(context.TODO(), name, obj, out, ttl)
	if err != nil {
		return err
	}
	return nil
}

func createPods(t *testing.T, helper *etcd3Helper, list *api.PodList) error {
	for i, pod := range list.Items {
		returnedObj := &api.Pod{}
		err := helper.Set(context.TODO(), pod.Name, &pod, returnedObj, 0)
		if err != nil {
			return err
		}
		list.Items[i] = *returnedObj
	}
	return nil
}

func testScheme(t *testing.T) (*runtime.Scheme, runtime.Codec) {
	scheme := runtime.NewScheme()
	scheme.Log(t)
	scheme.AddKnownTypes(*testapi.Default.GroupVersion(), &storagetesting.TestResource{})
	scheme.AddKnownTypes(testapi.Default.InternalGroupVersion(), &storagetesting.TestResource{})
	if err := scheme.AddConversionFuncs(
		func(in *storagetesting.TestResource, out *storagetesting.TestResource, s conversion.Scope) error {
			*out = *in
			return nil
		},
		func(in, out *time.Time, s conversion.Scope) error {
			*out = *in
			return nil
		},
	); err != nil {
		panic(err)
	}
	codec := serializer.NewCodecFactory(scheme).LegacyCodec(*testapi.Default.GroupVersion())
	return scheme, codec
}
