package common

import "errors"

var (
	ERR_LOCK_ALRADY_REQUIRE = errors.New("锁已被占用")
)
