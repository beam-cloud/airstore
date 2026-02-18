//go:build !darwin && !linux

package desktop

import "unsafe"

func nativeWindowShow(_ unsafe.Pointer) {}

func nativeWindowHide(_ unsafe.Pointer) {}
