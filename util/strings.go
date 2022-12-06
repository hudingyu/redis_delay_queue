package util

import (
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"html/template"
	"strconv"
	"strings"
)

func StringConv(s string, defaultValue interface{}, valueType string) interface{} {
	var err error
	var value interface{}
	switch valueType {
	case "bool":
		value, err = strconv.ParseBool(s)
	case "int":
		value, err = strconv.ParseInt(s, 10, 0)
		value = int(value.(int64))
	case "int8":
		value, err = strconv.ParseInt(s, 10, 8)
		value = int8(value.(int64))
	case "int16":
		value, err = strconv.ParseInt(s, 10, 16)
		value = int16(value.(int64))
	case "int32":
		value, err = strconv.ParseInt(s, 10, 32)
		value = int32(value.(int64))
	case "int64":
		value, err = strconv.ParseInt(s, 10, 64)
	case "float64":
		value, err = strconv.ParseFloat(s, 64)
	case "string":
		value = s
		if value == "" {
			value = defaultValue.(string)
		}
	case "[]string":
		value = strings.Fields(s)
	default:
		panic(fmt.Sprintf("nonexistent value type: %s", valueType))
	}
	if err != nil {
		value = defaultValue
	}
	return value
}

func StringPtrIfNotEmpty(input string) *string {
	if input == "" {
		return nil
	}

	return &input
}

func StringToArray(str string, separator string) []interface{} {
	array := strings.Split(str, separator)
	res := make([]interface{}, len(array))
	for i, val := range array {
		res[i] = val
	}
	return res
}

func ToJsonString(i interface{}) string {
	if i == nil {
		return ""
	}

	s, err := jsoniter.MarshalToString(i)
	if err != nil {
		return ""
	}
	if s == "null" {
		return ""
	}
	return s
}

func ToString(i interface{}) string {
	if i == nil {
		return ""
	}
	switch s := i.(type) {
	case string:
		return s
	case bool:
		return strconv.FormatBool(s)
	case float64:
		return strconv.FormatFloat(s, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(s), 'f', -1, 32)
	case int:
		return strconv.Itoa(s)
	case int64:
		return strconv.FormatInt(s, 10)
	case int32:
		return strconv.Itoa(int(s))
	case int16:
		return strconv.FormatInt(int64(s), 10)
	case int8:
		return strconv.FormatInt(int64(s), 10)
	case uint:
		return strconv.FormatInt(int64(s), 10)
	case uint64:
		return strconv.FormatInt(int64(s), 10)
	case uint32:
		return strconv.FormatInt(int64(s), 10)
	case uint16:
		return strconv.FormatInt(int64(s), 10)
	case uint8:
		return strconv.FormatInt(int64(s), 10)
	case []byte:
		return string(s)
	case template.HTML:
		return string(s)
	case template.URL:
		return string(s)
	case template.JS:
		return string(s)
	case template.CSS:
		return string(s)
	case template.HTMLAttr:
		return string(s)
	case nil:
		return ""
	case fmt.Stringer:
		return s.String()
	case error:
		return s.Error()
	default:
		return fmt.Sprint(i)
	}
}

func ToInt64Slice(i interface{}) []int64 {
	if i == nil {
		return nil
	}
	switch s := i.(type) {
	case []int64:
		return s
	case []int:
		res := make([]int64, len(s))
		for i, val := range s {
			res[i] = int64(val)
		}
		return res
	case []int32:
		res := make([]int64, len(s))
		for i, val := range s {
			res[i] = int64(val)
		}
		return res
	case []int16:
		res := make([]int64, len(s))
		for i, val := range s {
			res[i] = int64(val)
		}
		return res
	case []int8:
		res := make([]int64, len(s))
		for i, val := range s {
			res[i] = int64(val)
		}
		return res
	case []uint:
		res := make([]int64, len(s))
		for i, val := range s {
			res[i] = int64(val)
		}
		return res
	case []uint64:
		res := make([]int64, len(s))
		for i, val := range s {
			res[i] = int64(val)
		}
		return res
	case []uint32:
		res := make([]int64, len(s))
		for i, val := range s {
			res[i] = int64(val)
		}
		return res
	case []uint16:
		res := make([]int64, len(s))
		for i, val := range s {
			res[i] = int64(val)
		}
		return res
	case []uint8:
		res := make([]int64, len(s))
		for i, val := range s {
			res[i] = int64(val)
		}
		return res
	}
	return nil
}
