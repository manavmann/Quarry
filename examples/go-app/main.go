// Command go-app is the stdlib-only sample pipeline target: it counts
// words on stdin so the example pipeline has something to build, test,
// lint and package.
package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

// Count returns how often each whitespace-separated word occurs in r,
// case-insensitively.
func Count(r io.Reader) (map[string]int, error) {
	counts := map[string]int{}
	sc := bufio.NewScanner(r)
	sc.Split(bufio.ScanWords)
	for sc.Scan() {
		counts[strings.ToLower(sc.Text())]++
	}
	return counts, sc.Err()
}

// Top returns the n most frequent words, most frequent first; ties are
// broken alphabetically so the output is stable.
func Top(counts map[string]int, n int) []string {
	words := make([]string, 0, len(counts))
	for w := range counts {
		words = append(words, w)
	}
	sort.Slice(words, func(i, j int) bool {
		if counts[words[i]] != counts[words[j]] {
			return counts[words[i]] > counts[words[j]]
		}
		return words[i] < words[j]
	})
	if len(words) > n {
		words = words[:n]
	}
	return words
}

func main() {
	counts, err := Count(os.Stdin)
	if err != nil {
		fmt.Fprintln(os.Stderr, "go-app:", err)
		os.Exit(1)
	}
	for _, w := range Top(counts, 10) {
		fmt.Printf("%6d %s\n", counts[w], w)
	}
}
