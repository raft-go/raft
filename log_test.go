package raft

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestLog(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	t.Run("empty log", func(t *testing.T) {
		var log memoryLog
		t.Run("Get", func(t *testing.T) {
			cases := []struct {
				index, expect uint64
			}{
				{rand.Uint64(), 0},
				{rand.Uint64(), 0},
				{rand.Uint64(), 0},
				{rand.Uint64(), 0},
				{rand.Uint64(), 0},
			}

			for _, tc := range cases {
				got, err := log.Get(tc.index)
				if err != nil {
					t.Fatal(err)
				}
				if got != tc.expect {
					t.Errorf("Get(%d), expect %d but got %d", tc.index, tc.expect, got)
				}
			}
		})
		t.Run("Match", func(t *testing.T) {
			cases := []struct {
				index, term uint64
				expect      bool
			}{
				{rand.Uint64(), rand.Uint64(), false},
				{rand.Uint64(), rand.Uint64(), false},
				{rand.Uint64(), rand.Uint64(), false},
				{rand.Uint64(), rand.Uint64(), false},
			}

			for _, tc := range cases {
				got, err := log.Match(tc.index, tc.term)
				if err != nil {
					t.Fatal(err)
				}
				if got != tc.expect {
					t.Errorf("Match(%d, %d), expect %t but got %t", tc.index, tc.term, tc.expect, got)
				}
			}
		})
		t.Run("Last", func(t *testing.T) {
			gotIndex, gotTerm, err := log.Last()
			if err != nil {
				t.Fatal(err)
			}
			var expectIndex, expectTerm uint64
			if gotIndex != expectIndex {
				t.Errorf("expect index %d but got index %d", expectIndex, gotIndex)
			}
			if gotTerm != expectTerm {
				t.Errorf("expect term %d but got term %d", expectTerm, gotTerm)
			}
		})
		t.Run("RangeGet", func(t *testing.T) {
			cases := []struct {
				i, j uint64
			}{
				{0, 100},
				{1, 200},
			}

			for _, tc := range cases {
				got, err := log.RangeGet(tc.i, tc.j)
				if err != nil {
					t.Fatal(err)
				}
				expect := 0
				if len(got) != expect {
					t.Errorf("expect got %d but got %d", expect, len(got))
				}
			}
		})
	})
	t.Run("not empty log", func(t *testing.T) {
		var log memoryLog
		entries := []LogEntry{
			{Term: 0, Command: []byte("command at T0")},
			{Term: 1, Command: []byte("command at T1")},
			{Term: 2, Command: []byte("command at T2")},
		}
		err := log.Append(entries...)
		if err != nil {
			t.Fatal(err)
		}
		t.Run("Get", func(t *testing.T) {
			cases := []struct {
				index uint64
			}{
				{index: 0},
				{index: 1},
				{index: 2},
				{index: rand.Uint64()},
			}
			for _, tc := range cases {
				var expect uint64
				if tc.index >= 1 && tc.index <= uint64(len(entries)) {
					expect = entries[tc.index-1].Term
				}

				got, err := log.Get(tc.index)
				if err != nil {
					t.Fatal(err)
				}
				if got != expect {
					t.Errorf("expect %d but got %d", expect, got)
				}
			}
		})
		t.Run("Match", func(t *testing.T) {
			type matchCase struct {
				index, term uint64
				expect      bool
			}
			var cases []matchCase
			for i := uint64(0); i < 10; i++ {
				if i >= 1 && i <= uint64(len(entries)) {
					cases = append(cases, matchCase{
						index:  i,
						term:   entries[i-1].Term,
						expect: true,
					}, matchCase{
						index: i,
						term:  entries[i-1].Term + rand.Uint64() + 1,
					})
				}
			}
			for _, tc := range cases {
				t.Run(fmt.Sprintf("index %d term %d", tc.index, tc.term), func(t *testing.T) {
					got, err := log.Match(tc.index, tc.term)
					if err != nil {
						t.Fatal(err)
					}
					if got != tc.expect {
						t.Errorf("expect %t but got %t", tc.expect, got)
					}
				})
			}
		})
		t.Run("Last", func(t *testing.T) {
			gotIndex, gotTerm, err := log.Last()
			if err != nil {
				t.Fatal(err)
			}

			expect := entries[len(entries)-1]
			if gotIndex != expect.Index {
				t.Errorf("expect %d but got %d", expect.Index, gotIndex)
			}
			if gotTerm != expect.Term {
				t.Errorf("expect %d but got %d", expect.Term, gotTerm)
			}
		})
		t.Run("RangeGet", func(t *testing.T) {
			gotEntries, err := log.RangeGet(0, uint64(len(entries)))
			if err != nil {
				t.Fatal(err)
			}
			if len(gotEntries) != len(entries) {
				t.Errorf("expect %d but got %d", len(gotEntries), len(entries))
			}
			for i := 0; i < len(entries); i++ {
				got := gotEntries[i]
				expect := entries[i]
				if got.Term != expect.Term {
					t.Errorf("expect %d but got %d", expect.Term, got.Term)
				}
				if got.Index != expect.Index {
					t.Errorf("expect %d but got %d", expect.Index, got.Index)
				}
			}
		})
	})

}
