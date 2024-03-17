package wildcard

import (
	"errors"
	"regexp"
	"strings"
)

// Pattern 表示一个通配符模式
type Pattern struct {
	exp *regexp.Regexp
}

// 需要把通配符转换成正则字符串
var replaceMap = map[byte]string{
	'+': `\+`,
	')': `\)`,
	'$': `\$`,
	'.': `\.`,
	'{': `\{`,
	'}': `\}`,
	'|': `\|`,
	'*': `.*`,
	'?': `.`,
}

// CompilePattern convert wildcard string to Pattern
func CompilePattern(src string) (*Pattern, error) {
	regexSrc := strings.Builder{}
	regexSrc.WriteByte('^')
	for i := 0; i < len(src); i++ {
		ch := src[i]
		if ch == '\\' {
			if i == len(src)-1 {
				return nil, errors.New("\"end with escape \\\\")
			}
			regexSrc.WriteByte(ch)
			regexSrc.WriteByte(src[i+1])
			i++ // skip escaped character
		} else if ch == '^' {
			if i == 0 {
				regexSrc.WriteString(`\^`)
			} else if i == 1 {
				if src[i-1] == '[' {
					regexSrc.WriteString(`^`) // src is: [^
				} else {
					regexSrc.WriteString(`\^`)
				}
			} else {
				if src[i-1] == '[' && src[i-2] != '\\' {
					regexSrc.WriteString(`^`) // src is: [^, except \[^
				} else {
					regexSrc.WriteString(`\^`)
				}
			}
		} else if escaped, toEscape := replaceMap[ch]; toEscape {
			regexSrc.WriteString(escaped)
		} else {
			regexSrc.WriteByte(ch)
		}
	}
	regexSrc.WriteByte('$')
	re, err := regexp.Compile(regexSrc.String())
	if err != nil {
		return nil, err
	}
	return &Pattern{
		exp: re,
	}, nil
}

func (p *Pattern) IsMatch(s string) bool {
	return p.exp.MatchString(s)
}
