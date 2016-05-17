package app

import (
	"github.com/golang/glog"
	"golang.org/x/exp/inotify"
)

func listenForLockfileContention(path string, done chan struct{}) error {
	watcher, err := inotify.NewWatcher()
	if err != nil {
		glog.Errorf("unable to create watcher for lockfile: %v", err)
		return err
	}
	if err = watcher.AddWatch(path, inotify.IN_OPEN); err != nil {
		glog.Errorf("unable to watch lockfile: %v", err)
		return err
	}
	go func() {
		select {
		case ev := <-watcher.Event:
			glog.Infof("inotify event: %v", ev)
		case err = <-watcher.Error:
			glog.Errorf("inotify watcher error: %v", err)
		}
		close(done)
	}()
	return nil
}
