package test_helpers

import "regexp"

var whitespacePattern = regexp.MustCompile("\\s+")

func CompressWhitespace(in string) string {
	return whitespacePattern.ReplaceAllString(in, " ")
}
