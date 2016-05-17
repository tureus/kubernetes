// +build !linux

package app

import "errors"

func listenForLockfileContention(path string, done chan struct{}) error {
	return errors.New("kubelet unsupported in this build")
}
