package raft

import (
	"bytes"
	"fmt"
	"testing"
)

func TestMemoryStore(t *testing.T) {
	t.Run("Get-Set methods", func(t *testing.T) {
		var store memoryStore

		cases := []struct {
			key, value []byte
		}{
			{[]byte(""), []byte("")},
			{[]byte("first key"), []byte("first value")},
		}
		for _, tc := range cases {
			t.Run(fmt.Sprintf("key: %q value: %q", tc.key, tc.value), func(t *testing.T) {
				// before set kv
				got, err := store.Get(tc.key)
				if err != nil {
					t.Error(err)
				}
				expect := []byte{}
				if bytes.Compare(expect, got) != 0 {
					t.Errorf("expect %v, but got %v", expect, got)
				}

				// after set kv
				err = store.Set(tc.key, tc.value)
				if err != nil {
					t.Error(err)
				}
				got, err = store.Get(tc.key)
				if err != nil {
					t.Error(err)
				}
				expect = tc.value
				if bytes.Compare(expect, got) != 0 {
					t.Errorf("expect %v, but got %v", expect, got)
				}

				// reset
				expect = append(tc.value, []byte("-suffix")...)
				err = store.Set(tc.key, expect)
				if err != nil {
					t.Error(err)
				}
				got, err = store.Get(tc.key)
				if err != nil {
					t.Error(err)
				}
				if bytes.Compare(expect, got) != 0 {
					t.Errorf("expect %v, but got %v", expect, got)
				}
			})
		}
	})
	t.Run("GetUint64-SetUint64 methods", func(t *testing.T) {
		var store memoryStore

		cases := []struct {
			key   []byte
			value uint64
		}{
			{[]byte("first key"), 1},
			{[]byte("second key"), 2},
		}

		for _, tc := range cases {
			t.Run(fmt.Sprintf("key: %q value: %d", tc.key, tc.value), func(t *testing.T) {
				// before set
				got, err := store.GetUint64(tc.key)
				if err != nil {
					t.Error(err)
				}
				var expect uint64 = 0
				if expect != got {
					t.Errorf("expect: %d, but got: %d", expect, got)
				}
				// after set
				err = store.SetUint64(tc.key, tc.value)
				if err != nil {
					t.Error(err)
				}
				got, err = store.GetUint64(tc.key)
				if err != nil {
					t.Error(err)
				}
				expect = tc.value
				if expect != got {
					t.Errorf("expect: %d, but got: %d", expect, got)
				}
				// reset
				expect = tc.value + 1
				err = store.SetUint64(tc.key, expect)
				if err != nil {
					t.Error(err)
				}
				got, err = store.GetUint64(tc.key)
				if err != nil {
					t.Error(err)
				}
				if expect != got {
					t.Errorf("expect: %d, but got: %d", expect, got)
				}
			})
		}
	})
}
