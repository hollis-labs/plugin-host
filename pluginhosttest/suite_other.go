//go:build !unix

package pluginhosttest

const havePIDs = false

func pidExists(int) bool { return false }

func killPID(int) {}
